#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

from abc import ABCMeta, abstractmethod
from datetime import datetime
from collections import OrderedDict
from decimal import Decimal, setcontext
from sys import getsizeof, version_info

try:
    from sys import maxint as maxvalue
except ImportError:
    from sys import maxsize as maxvalue

from .common import ByteOutputStream, CheckValue, Empty, JsonNone, enum
from .exception import (
    IllegalArgumentException, IllegalStateException, NoSQLException,
    QueryException, QueryStateException, RetryableException)

try:
    from . import serde
except ImportError:
    import serde


class PlanIterState(object):
    STATE = enum(OPEN=0,
                 RUNNING=1,
                 DONE=2,
                 CLOSED=3)

    def __init__(self):
        self.state = PlanIterState.STATE.OPEN

    def close(self):
        self.set_state(PlanIterState.STATE.CLOSED)

    def done(self):
        self.set_state(PlanIterState.STATE.DONE)

    def is_close(self):
        return self.state == PlanIterState.STATE.CLOSED

    def is_done(self):
        return self.state == PlanIterState.STATE.DONE

    def is_open(self):
        return self.state == PlanIterState.STATE.OPEN

    def reset(self):
        self.set_state(PlanIterState.STATE.OPEN)

    def set_state(self, state):
        if self.state == PlanIterState.STATE.DONE:
            if (state == PlanIterState.STATE.CLOSED or
                    state == PlanIterState.STATE.OPEN):
                self.state = state
                return
        elif self.state == PlanIterState.STATE.RUNNING:
            if (state == PlanIterState.STATE.CLOSED or
                    state == PlanIterState.STATE.DONE or
                    state == PlanIterState.STATE.OPEN or
                    state == PlanIterState.STATE.RUNNING):
                self.state = state
                return
        elif self.state == PlanIterState.STATE.OPEN:
            """
            OPEN --> DONE transition is allowed for iterators that are 'done' on
            the 1st next() call after an open() or reset() call. In this case,
            rather than setting the state to RUNNING on entrance to the next()
            call and then setting the state again to DONE before returning from
            the same next() call, we allow a direct transition from OPEN to
            DONE.
            """
            if (state == PlanIterState.STATE.CLOSED or
                    state == PlanIterState.STATE.DONE or
                    state == PlanIterState.STATE.OPEN or
                    state == PlanIterState.STATE.RUNNING):
                self.state = state
                return
        raise QueryStateException(
            'Wrong state transition for iterator ' + str(self) +
            '. Current state: ' + self.state + ' New state: ' + state)


class AggrIterState(PlanIterState):

    def __init__(self):
        super(AggrIterState, self).__init__()
        self.count = 0
        self.sum = 0
        self.none_input_only = True
        self.min_max = None

    def reset(self):
        self.count = 0
        self.sum = 0
        self.none_input_only = True
        self.min_max = None


class PlanIter(object):
    """
    Base class for all query-plan iterators that may appear at the driver.

    The query compiler produces the "query execution plan" as a tree of plan
    iterators. Roughly speaking, each kind of iterator evaluates a different
    kind of expression that may appear in a query.

    Each plan iterator has an open()-next()-close() interface. To execute a
    query, the "user" must first call open() on the root iterator and then call
    next() a number of times to retrieve the results. Finally, after the
    application has retrieved all the results, or when it is not interested in
    retrieving any more results, it must call close() to release any resources
    held by the iterators.

    In general, these calls are propagated top-down within the execution plan,
    and results flow bottom-up. More specifically, each next() call on the root
    iterator produces one item (a field value) in the result set of the query.
    If there are no more results to be produced, the next() call will return
    False. Actually, the same is True for all iterators: each next() call on a
    plan iterator produces one item in the result set of that iterator or
    returns False if there are no more results. So, in the most general case,
    the result set of each iterator is a sequence of zero or more items.

    The root iterator will always produce dict values, but other iterators may
    produces different kinds of values.

    Iterator state and registers:

    As mentioned already, each next() call on an iterator produces at most one
    result item. However, the result items are not returned by the next() call
    directly. Instead, each iterator places its current result (the item
    produced by the current invocation of the next() call) in its "result
    register", where a consumer iterator can pick it up from. A "register" is
    just an entry in a list of field values. This array is created during the
    creation of the RuntimeControlBlock (RCB) and is stored in the RCB. Each
    iterator knows the position of its result register within the array, and
    will provide this info to its parent (consuming) iterator. All iterators
    have a result reg. Notice, however, that some iterators may share the same
    result reg. For example, if an iterator A is just filtering the result of
    another iterator B, A can use B's result reg as its own result reg as well.

    Each iterator has some state that it needs to maintain across open()-next()-
    close() invocations. For each kind of iterator, its state is represented by
    an instance of PlanIterState or subclass of. Like the result registers, the
    states of each iterator in the plan is stored in a PlanIterState array that
    is created during the creation of the RuntimeControlBlock (RCB) and is
    stored in the RCB. Each iterator knows the position of its state within this
    array.

    Storing the dynamic state of each iterator outside the iterator itself is
    important, because it makes the query plan immutable, and as a result, it
    allows multiple threads to concurrently execute the same query using a
    single, shared instance of the query plan, but with a different state per
    thread.

    The state of each iterator is created and initialized during the open()
    call, it is updated during subsequent next() calls, and is released during
    close(). Each iterator also has a reset() method, which is similar to
    open(): re-initializes the iterator state so that the iterator will produce
    a new result set from the beginning during subsequent next() calls.

    Data members:

    self.result_reg:\n
    The position within the array of registers, of the register where this
    iterator will store each item generated by a next() call.

    self.state_pos:\n
    The position, within the state array, where the state of this iterator is
    stored.

    self.location:\n
    The location, within the query text, of the expression implemented by this
    iterator. It is used only in error messages, to indicate the location within
    the query text of the expression that encountered the error.
    """
    __metaclass__ = ABCMeta

    TRACEDESER = False

    def __init__(self, bis):
        self.result_reg = PlanIter.read_positive_int(bis, True)
        self.state_pos = PlanIter.read_positive_int(bis)
        self.location = QueryException.Location(
            PlanIter.read_positive_int(bis), PlanIter.read_positive_int(bis),
            PlanIter.read_positive_int(bis), PlanIter.read_positive_int(bis))

    def display(self, output=None, formatter=None):
        if output is None and formatter is None:
            output = ''
            formatter = QueryFormatter()
        output = formatter.indent(output)
        if self.get_func_code() is not None:
            output += str(self.get_func_code())
        else:
            output += self.get_kind()
        output += '([' + str(self.result_reg) + '])\n'
        output = formatter.indent(output)
        output += '[\n'
        formatter.inc_indent()
        output = self.display_content(output, formatter)
        formatter.dec_indent()
        output += '\n'
        output = formatter.indent(output)
        output += ']'
        return output

    def display_regs(self, output):
        output += '([' + str(self.result_reg) + '])'
        return output

    def get_aggr_value(self, rcb, reset):
        """
        Get the current value of an aggregate function. If the reset param is
        True, the value is the final one and this method will also reset the
        state of the associated aggregate-function iterator. In this case the
        method is called when a group is completed. If reset is False, it is
        actually the value of the aggr function on the 1st tuple of a new group.
        """
        raise QueryStateException(
            'Method not implemented for iterator ' + self.get_kind())

    def get_func_code(self):
        """
        This method must be overridden by iterators that implement a family of
        builtin functions (and as a result have a FUNC_CODE data member).
        Currently, this method is used only in the display method below.
        """
        return None

    def get_input_iter(self):
        raise QueryStateException(
            'Method not implemented for iterator ' + self.get_kind())

    def get_location(self):
        return self.location

    def get_result_reg(self):
        return self.result_reg

    def get_state(self, rcb):
        return rcb.get_state(self.state_pos)

    def is_done(self, rcb):
        """
        Returns whether the iterator is in the DONE or CLOSED state. CLOSED is
        included because, in the order of states, a CLOSED iterator is also
        DONE.
        """
        state = rcb.get_state(self.state_pos)
        return state.is_done() or state.is_closed()

    @staticmethod
    def deserialize_iter(bis):
        kind = bis.read_byte()
        if kind == -1:
            return None
        value = PlanIter.PlanIterKind.value_of(kind)
        if PlanIter.TRACEDESER:
            print('Deserializing ' + value + ' iter.')
        if kind == PlanIter.PlanIterKind.ARITH_OP:
            op_iter = ArithOpIter(bis)
        elif kind == PlanIter.PlanIterKind.CONST:
            op_iter = ConstIter(bis)
        elif kind == PlanIter.PlanIterKind.EXTERNAL_VAR_REF:
            op_iter = ExternalVarRefIter(bis)
        elif kind == PlanIter.PlanIterKind.FIELD_STEP:
            op_iter = FieldStepIter(bis)
        elif kind == PlanIter.PlanIterKind.FN_MIN_MAX:
            op_iter = FuncMinMaxIter(bis)
        elif kind == PlanIter.PlanIterKind.FN_SUM:
            op_iter = FuncSumIter(bis)
        elif kind == PlanIter.PlanIterKind.GROUP:
            op_iter = GroupIter(bis)
        elif kind == PlanIter.PlanIterKind.RECV:
            op_iter = ReceiveIter(bis)
        elif kind == PlanIter.PlanIterKind.SFW:
            op_iter = SFWIter(bis)
        elif (kind == PlanIter.PlanIterKind.SORT or
              kind == PlanIter.PlanIterKind.SORT2):
            op_iter = SortIter(bis, kind)
        elif kind == PlanIter.PlanIterKind.VAR_REF:
            op_iter = VarRefIter(bis)
        else:
            raise QueryStateException('Unknown query iterator kind: ' + value)

        if PlanIter.TRACEDESER:
            print('Done Deserializing ' + value + ' iter')
        return op_iter

    @staticmethod
    def deserialize_iters(bis):
        iters = list()
        num_args = serde.BinaryProtocol.read_sequence_length(bis)
        count = 0
        while count < num_args:
            iters.append(PlanIter.deserialize_iter(bis))
            count += 1
        return iters

    @staticmethod
    def print_bytearray(byte_array):
        if byte_array is None:
            return 'None'
        output = ''
        output += '['
        for b in byte_array:
            output += str(b) + ' '
        output += ']'
        return output

    @staticmethod
    def read_ordinal(bis, num_values):
        """
        Read an ordinal number value and validate the value in the range
        0 ~ (num_values - 1).
        """
        index = bis.read_short_int()
        if index < 0 or index >= num_values:
            raise IllegalArgumentException(
                str(index) + 'is invalid, it must be in a range 0 ~ ' +
                str(num_values))
        return index

    @staticmethod
    def read_sort_specs(bis):
        length = serde.BinaryProtocol.read_sequence_length(bis)
        if length == -1:
            return None
        specs = list()
        count = 0
        while count < length:
            specs.append(SortSpec(bis))
            count += 1
        return specs

    @staticmethod
    def read_positive_int(bis, allow_neg_one=False):
        """Read an int value and check it."""
        value = bis.read_int()
        if allow_neg_one:
            if value < -1:
                raise IllegalArgumentException(
                    str(value) + ' is invalid, it must be a positive value ' +
                    'or -1.')
        else:
            if value < 0:
                raise IllegalArgumentException(
                    str(value) + ' is invalid, it must be a positive value.')
        return value

    @staticmethod
    def sizeof(value):
        return getsizeof(value)

    @abstractmethod
    def close(self, rcb):
        pass

    @abstractmethod
    def display_content(self, output, formatter):
        pass

    @abstractmethod
    def get_kind(self):
        pass

    @abstractmethod
    def next(self, rcb):
        pass

    @abstractmethod
    def open(self, rcb):
        pass

    @abstractmethod
    def reset(self, rcb):
        pass

    class PlanIterKind(object):
        """
        Enumeration of the different kinds of iterators (there is one PlanIter
        subclass for each kind).

        NOTE: The kvcode stored with each value in this enum matches the ordinal
        of the corresponding PlanIterKind in kvstore.
        """

        def __init__(self, kvcode):
            self._kvcode = kvcode

        CONST = 0
        VAR_REF = 1
        EXTERNAL_VAR_REF = 2
        ARITH_OP = 8
        FIELD_STEP = 11
        SFW = 14
        RECV = 17
        FN_SUM = 39
        FN_MIN_MAX = 41
        SORT = 47
        GROUP = 65
        SORT2 = 66

        VALUES_TO_NAMES = {0: 'CONST',
                           1: 'VAR_REF',
                           2: 'EXTERNAL_VAR_REF',
                           8: 'ARITH_OP',
                           11: 'FIELD_STEP',
                           14: 'SFW',
                           17: 'RECV',
                           39: 'FN_SUM',
                           41: 'FN_MIN_MAX',
                           47: 'SORT',
                           65: 'GROUP',
                           66: 'SORT2'}

        @staticmethod
        def value_of(kvcode):
            name = PlanIter.PlanIterKind.VALUES_TO_NAMES.get(kvcode)
            if name is None:
                print('Unexpected iterator kind: ' + str(kvcode))
            return name

    # Some iterator classes may implement more than one SQL builtin function. In
    # such cases, each particular instance of the iterator will store a
    # FUNC_CODE to specify what function is implemented by this instance.
    FUNC_CODE = enum(OP_ADD_SUB=14,
                     OP_MULT_DIV=15,
                     FN_COUNT_STAR=42,
                     FN_COUNT=43,
                     FN_COUNT_NUMBERS=44,
                     FN_SUM=45,
                     FN_MIN=47,
                     FN_MAX=48)


class ArithOpIter(PlanIter):
    """
    Iterator to implement the arithmetic operators

    any_atomic? ArithOp(any?, ....)

    An instance of this iterator implements either addition/subtraction among
    two or more input values, or multiplication/division among two or more input
    values. For example, arg1 + arg2 - arg3 + arg4,
    or arg1 * arg2 * arg3 / arg4.

    The only arithmetic op that is strictly needed for the driver is the div
    (real division) op, to compute an AVG aggregate function as the division of
    a SUM by a COUNT. However, having all the arithmetic ops implemented allows
    for expressions in the SELECT list that do arithmetic among aggregate
    functions (for example: select a, sum(x) + sum(y) from foo group by a).
    """

    def __init__(self, bis):
        super(ArithOpIter, self).__init__(bis)
        self._ordinal = bis.read_short_int()
        self._code = self._ordinal
        """
        Whether this iterator performs addition/subtraction or
        multiplication/division.
        """
        self._args = self.deserialize_iters(bis)
        self._ops = serde.BinaryProtocol.read_string(bis)
        """
        If self._code == PlanIter.FUNC_CODE.OP_ADD_SUB, self._ops is a string of
        '+' and/or '-' chars, containing one such char per input value. For
        example, if the arithmetic expression is (arg1 + arg2 - arg3 + arg4)
        self._ops is '++-+'.

        If self._code == PlanIter.FUNC_CODE.OP_MULT_DIV, self._ops is a string
        of '*', '/', and/or 'd' chars, containing one such char per input value.
        For example, if the arithmetic expression is (arg1 * arg2 * arg3 / arg4)
        self._ops is '***/'. The 'd' char is used for the div operator.
        """
        self._init_result = (
            0 if self._code == PlanIter.FUNC_CODE.OP_ADD_SUB else 1)
        # Whether div is any of the operations to be performed by this
        # ArithOpIter.
        self._have_real_div = self._ops.find('d') != -1
        assert len(self._ops) == len(self._args), \
            ('Not enough operations: ops:' + str(len(self._ops) - 1) +
             ' args:' + str(len(self._args)))

    def get_func_code(self):
        return self._code

    def close(self, rcb):
        state = rcb.get_state(self.state_pos)
        if state is None:
            return
        for arg_iter in self._args:
            arg_iter.close(rcb)
        state.close()

    def display_content(self, output, formatter):
        for i in range(len(self._args)):
            output = formatter.indent(output)
            if self._code == PlanIter.FUNC_CODE.OP_ADD_SUB:
                if self._ops[i] == '+':
                    output += '+'
                else:
                    output += '-'
            else:
                if self._ops[i] == '*':
                    output += '*'
                else:
                    output += '/'
            output += ',\n'
            output = self._args[i].display(output, formatter)
            if i < len(self._args) - 1:
                output += ',\n'
        return output

    def get_kind(self):
        return PlanIter.PlanIterKind.value_of(ArithOpIter.PlanIterKind.ARITH_OP)

    def next(self, rcb):
        state = rcb.get_state(self.state_pos)
        if state.is_done():
            return False
        res_type = (
            serde.BinaryProtocol.FIELD_VALUE_TYPE.DOUBLE if self._have_real_div
            else serde.BinaryProtocol.FIELD_VALUE_TYPE.INTEGER)
        # Determine the type of the result for the expression by iterating its
        # components, enforcing the promotion rules for numeric types.
        #
        # Start with INTEGER, unless we have any div operator, in which case
        # start with DOUBLE.
        for i in range(len(self._args)):
            arg_iter = self._args[i]
            op_next = arg_iter.next(rcb)
            if not op_next:
                state.done()
                return False
            arg_val = rcb.get_reg_val(arg_iter.get_result_reg())
            if arg_val is None:
                res = None
                rcb.set_reg_val(self.result_reg, res)
                state.done()
                return True
            if isinstance(arg_val, float):
                if (res_type == serde.BinaryProtocol.FIELD_VALUE_TYPE.INTEGER or
                        res_type == serde.BinaryProtocol.FIELD_VALUE_TYPE.LONG):
                    res_type = serde.BinaryProtocol.FIELD_VALUE_TYPE.DOUBLE
            elif isinstance(arg_val, int):
                pass
            elif version_info.major == 2 and CheckValue.is_long(arg_val):
                if res_type == serde.BinaryProtocol.FIELD_VALUE_TYPE.INTEGER:
                    res_type = serde.BinaryProtocol.FIELD_VALUE_TYPE.LONG
            elif isinstance(arg_val, Decimal):
                res_type = serde.BinaryProtocol.FIELD_VALUE_TYPE.NUMBER
            else:
                raise QueryException(
                    'Operand in arithmetic operation has illegal type\n' +
                    'Operand : ' + str(i) + ' type :\n' + str(type(arg_val)),
                    self.get_location())
        if res_type == serde.BinaryProtocol.FIELD_VALUE_TYPE.DOUBLE:
            res = float(self._init_result)
        elif res_type == serde.BinaryProtocol.FIELD_VALUE_TYPE.INTEGER:
            res = self._init_result
        elif res_type == serde.BinaryProtocol.FIELD_VALUE_TYPE.LONG:
            res = long(self._init_result)
        elif res_type == serde.BinaryProtocol.FIELD_VALUE_TYPE.NUMBER:
            res = None
        else:
            raise QueryStateException(
                'Invalid result type code: ' + str(res_type))
        for i in range(len(self._args)):
            arg_iter = self._args[i]
            arg_val = rcb.get_reg_val(arg_iter.get_result_reg())
            assert arg_val is not None
            if self._code == PlanIter.FUNC_CODE.OP_ADD_SUB:
                if self._ops[i] == '+':
                    if ((res_type ==
                         serde.BinaryProtocol.FIELD_VALUE_TYPE.DOUBLE) or
                            (res_type ==
                             serde.BinaryProtocol.FIELD_VALUE_TYPE.INTEGER) or
                            (res_type ==
                             serde.BinaryProtocol.FIELD_VALUE_TYPE.LONG)):
                        res += arg_val
                    elif (res_type ==
                          serde.BinaryProtocol.FIELD_VALUE_TYPE.NUMBER):
                        if res is None:
                            res = arg_val
                        else:
                            res += arg_val
                else:
                    if ((res_type ==
                         serde.BinaryProtocol.FIELD_VALUE_TYPE.DOUBLE) or
                            (res_type ==
                             serde.BinaryProtocol.FIELD_VALUE_TYPE.INTEGER) or
                            (res_type ==
                             serde.BinaryProtocol.FIELD_VALUE_TYPE.LONG)):
                        res -= arg_val
                    elif (res_type ==
                          serde.BinaryProtocol.FIELD_VALUE_TYPE.NUMBER):
                        if res is None:
                            res = -arg_val
                        else:
                            res -= arg_val
            else:
                if self._ops[i] == '*':
                    if ((res_type ==
                         serde.BinaryProtocol.FIELD_VALUE_TYPE.DOUBLE) or
                            (res_type ==
                             serde.BinaryProtocol.FIELD_VALUE_TYPE.INTEGER) or
                            (res_type ==
                             serde.BinaryProtocol.FIELD_VALUE_TYPE.LONG)):
                        res *= arg_val
                    elif (res_type ==
                          serde.BinaryProtocol.FIELD_VALUE_TYPE.NUMBER):
                        if res is None:
                            res = arg_val
                        else:
                            res *= arg_val
                else:
                    if ((res_type ==
                         serde.BinaryProtocol.FIELD_VALUE_TYPE.DOUBLE) or
                            (res_type ==
                             serde.BinaryProtocol.FIELD_VALUE_TYPE.INTEGER) or
                            (res_type ==
                             serde.BinaryProtocol.FIELD_VALUE_TYPE.LONG)):
                        res /= arg_val
                    elif (res_type ==
                          serde.BinaryProtocol.FIELD_VALUE_TYPE.NUMBER):
                        if res is None:
                            res = Decimal(1)
                        else:
                            res /= arg_val
        rcb.set_reg_val(self.result_reg, res)
        state.done()
        return True

    def open(self, rcb):
        rcb.set_state(self.state_pos, PlanIterState())
        for arg_iter in self._args:
            arg_iter.open(rcb)

    def reset(self, rcb):
        for arg_iter in self._args:
            arg_iter.reset(rcb)
        state = rcb.get_state(self.state_pos)
        state.reset()


class ConstIter(PlanIter):
    """
    ConstIter represents a reference to a constant value in the query. Such a
    reference will need to be "executed" at the driver side when the constant
    appears in the OFFSET or LIMIT clause.
    """

    def __init__(self, bis):
        super(ConstIter, self).__init__(bis)
        self._value = serde.BinaryProtocol.read_field_value(bis)

    def close(self, rcb):
        state = rcb.get_state(self.state_pos)
        if state is None:
            return
        state.close()

    def display_content(self, output, formatter):
        output = formatter.indent(output)
        output += str(self._value)
        return output

    def get_kind(self):
        return PlanIter.PlanIterKind.value_of(ArithOpIter.PlanIterKind.CONST)

    def get_value(self):
        return self._value

    def next(self, rcb):
        state = rcb.get_state(self.state_pos)
        if state.is_done():
            return False
        state.done()
        return True

    def open(self, rcb):
        rcb.set_state(self.state_pos, PlanIterState())
        rcb.set_reg_val(self.result_reg, self._value)

    def reset(self, rcb):
        state = rcb.get_state(self.state_pos)
        state.reset()


class ExternalVarRefIter(PlanIter):
    """
    In general, ExternalVarRefIter represents a reference to an external
    variable in the query. Such a reference will need to be "executed" at the
    driver side when the variable appears in the OFFSET or LIMIT clause.

    ExternalVarRefIter simply returns the value that the variable is currently
    bound to. This value is set by the app via the methods of QueryRequest.

    self._name:\n
    The name of the variable. Used only when displaying the execution plan and
    in error messages.

    self._id:\n
    The variable id. It is used as an index into a list of field values in the
    RCB that stores the values of the external vars.
    """

    def __init__(self, bis):
        super(ExternalVarRefIter, self).__init__(bis)
        self._name = serde.BinaryProtocol.read_string(bis)
        self._id = PlanIter.read_positive_int(bis)

    def close(self, rcb):
        state = rcb.get_state(self.state_pos)
        if state is None:
            return
        state.close()

    def display(self, output=None, formatter=None):
        if output is None and formatter is None:
            output = ''
            formatter = QueryFormatter()
        output = formatter.indent(output)
        output = self.display_content(output, formatter)
        output = self.display_regs(output)
        return output

    def display_content(self, output, formatter):
        output += 'EXTERNAL_VAR_REF(' + self._name + ', ' + str(self._id) + ')'
        return output

    def get_kind(self):
        return PlanIter.PlanIterKind.value_of(
            ArithOpIter.PlanIterKind.EXTERNAL_VAR_REF)

    def next(self, rcb):
        state = rcb.get_state(self.state_pos)
        if state.is_done():
            return False
        val = rcb.get_external_var(self._id)
        # val should not be none, because we check before starting query
        # execution that all the external vars have been bound. So this is a
        # sanity check.* val should not be none, because we check before
        # starting query execution that all the external vars have been bound.
        # So this is a sanity check.
        if val is None:
            raise QueryStateException(
                'Variable ' + self._name + ' has not been set.')
        rcb.set_reg_val(self.result_reg, val)
        state.done()
        return True

    def open(self, rcb):
        rcb.set_state(self.state_pos, PlanIterState())

    def reset(self, rcb):
        state = rcb.get_state(self.state_pos)
        state.reset()


class FieldStepIter(PlanIter):
    """
    FieldStepIter returns the value of a field in an input dict. It is used by
    the driver to implement column references in the SELECT list (see SFWIter).
    """

    def __init__(self, bis):
        super(FieldStepIter, self).__init__(bis)
        self._input_iter = self.deserialize_iter(bis)
        self._field_name = serde.BinaryProtocol.read_string(bis)

    def close(self, rcb):
        state = rcb.get_state(self.state_pos)
        if state is None:
            return
        self._input_iter.close(rcb)
        state.close()

    def display_content(self, output, formatter):
        output = self._input_iter.display(output, formatter)
        output += ', \n'
        output = formatter.indent(output)
        output += self._field_name
        return output

    def get_kind(self):
        return PlanIter.PlanIterKind.value_of(
            ArithOpIter.PlanIterKind.FIELD_STEP)

    def open(self, rcb):
        rcb.set_state(self.state_pos, PlanIterState())
        self._input_iter.open(rcb)

    def reset(self, rcb):
        self._input_iter.reset(rcb)
        state = rcb.get_state(self.state_pos)
        state.reset()

    def next(self, rcb):
        state = rcb.get_state(self.state_pos)
        if state.is_done():
            return False
        input_reg = self._input_iter.get_result_reg()
        while True:
            more = self._input_iter.next(rcb)
            ctx_item = rcb.get_reg_val(input_reg)
            if not more:
                state.done()
                return False
            if isinstance(ctx_item, list):
                raise QueryStateException(
                    'Input value in field step has invalid type.\n' +
                    str(ctx_item))
            if not isinstance(ctx_item, dict):
                continue
            try:
                result = ctx_item[self._field_name]
            except KeyError:
                continue
            rcb.set_reg_val(self.result_reg, result)
            return True


class FuncMinMaxIter(PlanIter):
    """
    any_atomic min(any*)
    any_atomic max(any*)

    Implements the MIN/MAX aggregate functions. It is needed by the driver to
    compute the total min/max from the partial mins/maxs received from the
    proxy.
    """

    def __init__(self, bis):
        super(FuncMinMaxIter, self).__init__(bis)
        self._func_code = bis.read_short_int()
        self._input_iter = self.deserialize_iter(bis)

    def close(self, rcb):
        state = rcb.get_state(self.state_pos)
        if state is None:
            return
        self._input_iter.close(rcb)
        state.close()

    def display_content(self, output, formatter):
        return self._input_iter.display(output, formatter)

    def get_aggr_value(self, rcb, reset):
        state = rcb.get_state(self.state_pos)
        res = state.min_max
        if reset:
            state.reset()
        return res

    def get_func_code(self):
        return self._func_code

    def get_input_iter(self):
        return self._input_iter

    def get_kind(self):
        return PlanIter.PlanIterKind.value_of(
            ArithOpIter.PlanIterKind.FN_MIN_MAX)

    def next(self, rcb):
        # raise Exception()
        state = rcb.get_state(self.state_pos)
        if state is None:
            return False
        while True:
            more = self._input_iter.next(rcb)
            if not more:
                return True
            val = rcb.get_reg_val(self._input_iter.get_result_reg())
            FuncMinMaxIter._minmax_new_val(rcb, state, self._func_code, val)

    def open(self, rcb):
        rcb.set_state(self.state_pos, AggrIterState())
        self._input_iter.open(rcb)

    def reset(self, rcb):
        # Don't reset the state of 'self'. Resetting the state is done in method
        # get_aggr_value above.
        self._input_iter.reset(rcb)

    @staticmethod
    def _minmax_new_val(rcb, state, fncode, val):
        if (val is None or isinstance(val, bytearray) or isinstance(val, dict)
                or isinstance(val, list) or isinstance(val, Empty) or
                isinstance(val, JsonNone)):
            return
        if state.min_max is None:
            state.min_max = val
            return
        comp = Compare.compare_atomics(rcb, state.min_max, val, True)
        if rcb.get_trace_level() >= 3:
            rcb.trace('Compared values: \n' + str(state.min_max) + '\n' +
                      str(val) + '\ncomp res = ' + str(comp))
        if fncode == PlanIter.FUNC_CODE.FN_MIN:
            if comp <= 0:
                return
        else:
            if comp >= 0:
                return
        if rcb.get_trace_level() >= 2:
            rcb.trace('Setting min/max to ' + str(val))
        state.min_max = val


class FuncSumIter(PlanIter):
    """
    any_atomic sum(any*)

    Implements the SUM aggregate function. It is needed by the driver to re-sum
    partial sums and counts received from the proxy.

    Note: The next() method does not actually return a value; it just adds a new
    value (if it is of a numeric type) to the running sum kept in the state.
    Also the reset() method resets the input iter (so that the next input value
    can be computed), but does not reset the FuncSumState. The state is reset,
    and the current sum value is returned, by the get_aggr_value() method.
    """

    def __init__(self, bis):
        super(FuncSumIter, self).__init__(bis)
        self._input_iter = self.deserialize_iter(bis)

    def close(self, rcb):
        state = rcb.get_state(self.state_pos)
        if state is None:
            return
        self._input_iter.close(rcb)
        state.close()

    def display_content(self, output, formatter):
        return self._input_iter.display(output, formatter)

    def get_aggr_value(self, rcb, reset):
        """
        This method is called twice when a group completes and a new group
        starts. In both cases it returns the current value of the SUM that is
        stored in the FuncSumState. The 1st time, the SUM value is the final SUM
        value for the just completed group. In this case the "reset" param is
        True in order to reset the running sum in the state. The 2nd time the
        SUM value is the initial SUM value computed from the 1st tuple of the
        new group.
        """
        state = rcb.get_state(self.state_pos)
        if state.none_input_only:
            return None
        res = state.sum
        if rcb.get_trace_level() >= 4:
            rcb.trace('Computed sum = ' + str(res))
        if reset:
            state.reset()
        return res

    def get_kind(self):
        return PlanIter.PlanIterKind.value_of(ArithOpIter.PlanIterKind.FN_SUM)

    def get_input_iter(self):
        return self._input_iter

    def next(self, rcb):
        state = rcb.get_state(self.state_pos)
        if state.is_done():
            return False
        while True:
            more = self._input_iter.next(rcb)
            if not more:
                return True
            val = rcb.get_reg_val(self._input_iter.get_result_reg())
            if rcb.get_trace_level() >= 2:
                rcb.trace('Summing up value ' + str(val))
            if val is None:
                continue
            state.none_input_only = False
            FuncSumIter._sum_new_value(state, val)

    def open(self, rcb):
        rcb.set_state(self.state_pos, AggrIterState())
        self._input_iter.open(rcb)

    def reset(self, rcb):
        # Don't reset the state of 'self'. Resetting the state is done in method
        # get_aggr_value above.
        self._input_iter.reset(rcb)

    @staticmethod
    def _sum_new_value(state, val):
        if CheckValue.is_digit(val):
            state.count += 1
            state.sum += val


class GroupIter(PlanIter):

    def __init__(self, bis):
        super(GroupIter, self).__init__(bis)
        self._input = self.deserialize_iter(bis)
        self.num_gb_columns = bis.read_int()
        self._column_names = serde.BinaryProtocol.read_string_array(bis)
        num_aggrs = len(self._column_names) - self.num_gb_columns
        self._aggr_funcs = [0] * num_aggrs
        for i in range(num_aggrs):
            self._aggr_funcs[i] = bis.read_short_int()
        self._is_distinct = bis.read_boolean()
        self._remove_produced_result = bis.read_boolean()
        self._count_memory = bis.read_boolean()

    def close(self, rcb):
        state = rcb.get_state(self.state_pos)
        if state is None:
            return
        self._input.close(rcb)
        state.close()

    def display_content(self, output, formatter):
        output = formatter.indent(output)
        output += 'Grouping Columns : '
        for i in range(self.num_gb_columns):
            output += self._column_names[i]
            if i < self.num_gb_columns - 1:
                output += ', '
        output += '\n'
        output = formatter.indent(output)
        output += 'Aggregate Functions : '
        for i in range(len(self._aggr_funcs)):
            output += str(self._aggr_funcs[i])
            if i < len(self._aggr_funcs) - 1:
                output += ',\n'
        output += '\n'
        self._input.display(output, formatter)
        return output

    def get_input_iter(self):
        return self._input

    def get_kind(self):
        return PlanIter.PlanIterKind.value_of(ArithOpIter.PlanIterKind.GROUP)

    def next(self, rcb):
        state = rcb.get_state(self.state_pos)
        if state.is_done():
            return False
        while True:
            if state.results_copy is not None:
                try:
                    (gb_tuple, aggr_tuple) = (
                        state.results_copy.popitem(last=False))
                    res = dict()
                    i = 0
                    while i < self.num_gb_columns:
                        res[self._column_names[i]] = gb_tuple.values[i]
                        i += 1
                    for i in range(i, len(self._column_names)):
                        aggr = self._get_aggr_value(aggr_tuple, i)
                        res[self._column_names[i]] = aggr
                    res = serde.BinaryProtocol.convert_value_to_none(res)
                    rcb.set_reg_val(self.result_reg, res)
                    if self._remove_produced_result:
                        state.results.pop(gb_tuple)
                    return True
                except KeyError:
                    # Dictionary is empty.
                    state.done()
                    return False
            more = self._input.next(rcb)
            if not more:
                if rcb.reached_limit():
                    return False
                if self.num_gb_columns == len(self._column_names):
                    state.done()
                    return False
                state.results_copy = state.results.copy()
                continue
            in_tuple = rcb.get_reg_val(self._input.get_result_reg())
            i = 0
            while i < self.num_gb_columns:
                col_value = in_tuple.get(self._column_names[i])
                if isinstance(col_value, Empty):
                    if self._is_distinct:
                        col_value = None
                    else:
                        break
                state.gb_tuple.values[i] = col_value
                i += 1
            if i < self.num_gb_columns:
                continue
            aggr_tuple = state.results.get(state.gb_tuple)
            if aggr_tuple is None:
                num_aggr_columns = (
                        len(self._column_names) - self.num_gb_columns)
                gb_tuple = GroupIter.GroupTuple(self.num_gb_columns)
                aggr_tuple = list()
                aggr_tuple_size = 0
                for i in range(num_aggr_columns):
                    val = GroupIter.AggrValue(self._aggr_funcs[i])
                    aggr_tuple.append(val)
                    if self._count_memory:
                        aggr_tuple_size += self.sizeof(val)
                i = 0
                while i < self.num_gb_columns:
                    gb_tuple.values[i] = state.gb_tuple.values[i]
                    i += 1
                if self._count_memory:
                    sz = self.sizeof(gb_tuple) + aggr_tuple_size
                    rcb.inc_memory_consumption(sz)
                for i in range(i, len(self._column_names)):
                    self._aggregate(rcb, aggr_tuple, i,
                                    in_tuple.get(self._column_names[i]))
                state.results[gb_tuple] = aggr_tuple
                if rcb.get_trace_level() >= 3:
                    rcb.trace('Started new group:\n' +
                              GroupIter._print_result(gb_tuple, aggr_tuple))
                if self.num_gb_columns == len(self._column_names):
                    res = dict()
                    for i in range(self.num_gb_columns):
                        res[self._column_names[i]] = gb_tuple.values[i]
                    res = serde.BinaryProtocol.convert_value_to_none(res)
                    rcb.set_reg_val(self.result_reg, res)
                    return True
            else:
                for i in range(self.num_gb_columns, len(self._column_names)):
                    self._aggregate(rcb, aggr_tuple, i,
                                    in_tuple.get(self._column_names[i]))
                if rcb.get_trace_level() >= 3:
                    rcb.trace('Updated existing group:\n' +
                              self._print_result(state.gb_tuple, aggr_tuple))

    def open(self, rcb):
        rcb.set_state(self.state_pos, GroupIter.GroupIterState(self))
        self._input.open(rcb)

    def reset(self, rcb):
        state = rcb.get_state(self.state_pos)
        state.reset()
        self._input.reset(rcb)

    def _aggregate(self, rcb, aggr_values, column, val):
        aggr_value = aggr_values[column - self.num_gb_columns]
        aggr_kind = self._aggr_funcs[column - self.num_gb_columns]
        if aggr_kind == PlanIter.FUNC_CODE.FN_COUNT:
            if val is None:
                return
            aggr_value.add(rcb, self._count_memory, 1, rcb.get_math_context())
        elif aggr_kind == PlanIter.FUNC_CODE.FN_COUNT_NUMBERS:
            if val is None or not CheckValue.is_digit(val):
                return
            aggr_value.add(rcb, self._count_memory, 1, rcb.get_math_context())
        elif aggr_kind == PlanIter.FUNC_CODE.FN_COUNT_STAR:
            aggr_value.add(rcb, self._count_memory, 1, rcb.get_math_context())
        elif aggr_kind == PlanIter.FUNC_CODE.FN_SUM:
            if val is None:
                return
            if CheckValue.is_digit(val):
                aggr_value.add(rcb, self._count_memory, val,
                               rcb.get_math_context())
        elif (aggr_kind == PlanIter.FUNC_CODE.FN_MAX or
              aggr_kind == PlanIter.FUNC_CODE.FN_MIN):
            if (val is None or isinstance(val, bytearray) or
                    isinstance(val, dict) or isinstance(val, list) or
                    isinstance(val, Empty) or isinstance(val, JsonNone)):
                return
            if aggr_value.value is None:
                if rcb.get_trace_level() >= 3:
                    rcb.trace('Setting min/max to ' + str(val))
                if self._count_memory:
                    rcb.inc_memory_consumption(
                        self.sizeof(val) - self.sizeof(aggr_value.value))
                aggr_value.value = val
                return
            comp = Compare.compare_atomics(rcb, aggr_value.value, val, True)
            if rcb.get_trace_level() >= 3:
                rcb.trace('Compared values: \n' + str(aggr_value.value) + '\n' +
                          str(val) + '\ncomp res = ' + str(comp))
            if aggr_kind == PlanIter.FUNC_CODE.FN_MIN:
                if comp <= 0:
                    return
            else:
                if comp >= 0:
                    return
            if rcb.get_trace_level() >= 3:
                rcb.trace('Setting min/max to ' + str(val))
            if (self._count_memory and
                    not isinstance(val, type(aggr_value.value))):
                rcb.inc_memory_consumption(
                    self.sizeof(val) - self.sizeof(aggr_value.value))
            aggr_value.value = val
        else:
            raise QueryStateException(
                'Method not implemented for iterator ' + str(aggr_kind))

    def _get_aggr_value(self, aggr_tuple, column):
        aggr_value = aggr_tuple[column - self.num_gb_columns]
        aggr_kind = self._aggr_funcs[column - self.num_gb_columns]
        if (aggr_kind == PlanIter.FUNC_CODE.FN_SUM and
                not aggr_value.got_numeric_input):
            return None
        return aggr_value.value

    @staticmethod
    def _print_result(gb_tuple, aggr_values):
        output = '['
        for i in range(len(gb_tuple.values)):
            output += str(gb_tuple.values[i]) + ' '
        output += ' - '
        for i in range(len(aggr_values)):
            output += str(aggr_values[i].value) + ' '
        output += ']'
        return output

    class AggrValue(object):

        def __init__(self, kind):
            self.got_numeric_input = False
            if (kind == PlanIter.FUNC_CODE.FN_COUNT or
                    kind == PlanIter.FUNC_CODE.FN_COUNT_NUMBERS or
                    kind == PlanIter.FUNC_CODE.FN_COUNT_STAR or
                    kind == PlanIter.FUNC_CODE.FN_SUM):
                self.value = 0
            elif (kind == PlanIter.FUNC_CODE.FN_MAX or
                  kind == PlanIter.FUNC_CODE.FN_MIN):
                self.value = None
            else:
                assert False

        def add(self, rcb, count_memory, val, ctx):
            setcontext(ctx)
            sz = 0
            if CheckValue.is_int(val) or CheckValue.is_long(val):
                self.got_numeric_input = True
                if CheckValue.is_digit(self.value):
                    self.value += val
                else:
                    assert False
            elif isinstance(val, float):
                self.got_numeric_input = True
                if (CheckValue.is_int(self.value) or
                        CheckValue.is_long(self.value)):
                    if count_memory:
                        sz = PlanIter.sizeof(self.value)
                    self.value += val
                    if count_memory:
                        rcb.inc_memory_consumption(
                            PlanIter.sizeof(self.value) - sz)
                elif (isinstance(self.value, float) or
                      isinstance(self.value, Decimal)):
                    self.value += val
                else:
                    assert False
            elif isinstance(val, Decimal):
                self.got_numeric_input = True
                if (CheckValue.is_int(self.value) or
                        CheckValue.is_long(self.value) or
                        isinstance(self.value, float)):
                    if count_memory:
                        sz = PlanIter.sizeof(self.value)
                    self.value += val
                    if count_memory:
                        rcb.inc_memory_consumption(
                            PlanIter.sizeof(self.value) - sz)
                elif isinstance(self.value, Decimal):
                    self.value += val
                else:
                    assert False
            else:
                assert False

    class GroupIterState(PlanIterState):

        def __init__(self, op_iter):
            super(GroupIter.GroupIterState, self).__init__()
            self.gb_tuple = GroupIter.GroupTuple(op_iter.num_gb_columns)
            self.results = OrderedDict()
            self.results_copy = None

        def close(self):
            super(GroupIter.GroupIterState, self).close()
            self.gb_tuple = None
            self.results.clear()
            self.results_copy = None

        def done(self):
            super(GroupIter.GroupIterState, self).done()
            self.gb_tuple = None
            self.results.clear()
            self.results_copy = None

        def reset(self):
            super(GroupIter.GroupIterState, self).reset()
            self.results.clear()
            self.results_copy = None

    class GroupTuple(object):

        def __init__(self, num_gb_columns):
            self.values = [0] * num_gb_columns

        def __eq__(self, other):
            for i in range(len(self.values)):
                if self.values[i] != other.values[i]:
                    return False
            return True

        def __hash__(self):
            code = 1
            for val in self.values:
                code += 31 * code + Compare.hashcode(val)
            return code


class ReceiveIter(PlanIter):
    """
    ReceiveIter requests and receives results from the proxy. For sorting
    queries, it performs a merge sort of the received results. It also performs
    duplicate elimination for queries that require it (note: a query can do both
    sorting and dup elimination).
    """
    DISTRIBUTION_KIND = enum(
        # The query predicates specify a complete shard key, and as a result,
        # the query goes to a single partition and uses the primary index for
        # its execution.
        SINGLE_PARTITION=0,
        # The query uses the primary index for its execution, but does not
        # specify a complete shard key. As a result, it must be sent to all
        # partitions.
        ALL_PARTITIONS=1,
        # The query uses a secondary index for its execution. As a result, it
        # must be sent to all shards.
        ALL_SHARDS=2)

    def __init__(self, bis):
        super(ReceiveIter, self).__init__(bis)
        # The distribution kind of the query.
        self.distribution_kind = bis.read_short_int()
        # Used for sorting queries. It specifies the names of the top-level
        # fields that contain the values on which to sort the received results.
        self.sort_fields = serde.BinaryProtocol.read_string_array(bis)
        self.sort_specs = PlanIter.read_sort_specs(bis)
        # Used for duplicate elimination. It specifies the names of the
        # top-level fields that contain the primary-key values within the
        # received results .
        self._prim_key_fields = serde.BinaryProtocol.read_string_array(bis)

    def close(self, rcb):
        state = rcb.get_state(self.state_pos)
        if state is None:
            return
        state.close()

    def display_content(self, output, formatter):
        output = formatter.indent(output)
        output += 'DistributionKind : ' + str(self.distribution_kind) + ',\n'
        if self.sort_fields is not None:
            output = formatter.indent(output)
            output += 'Sort Fields : '
            num_sort_fields = len(self.sort_fields)
            for i in range(num_sort_fields):
                output += self.sort_fields[i]
                if i < num_sort_fields - 1:
                    output += ', '
            output += ',\n'
        if self._prim_key_fields is not None:
            output = formatter.indent(output)
            output += 'Primary Key Fields : '
            num_primkey_fields = len(self._prim_key_fields)
            for i in range(num_primkey_fields):
                output += self._prim_key_fields[i]
                if i < num_primkey_fields - 1:
                    output += ', '
            output += ',\n'
        return output

    def does_dup_elim(self):
        return self._prim_key_fields is not None

    def does_sort(self):
        return self.sort_fields is not None

    def get_kind(self):
        return PlanIter.PlanIterKind.value_of(ArithOpIter.PlanIterKind.RECV)

    def next(self, rcb):
        state = rcb.get_state(self.state_pos)
        if state.is_done():
            if rcb.get_trace_level() >= 1:
                rcb.trace('ReceiveIter.next() : done')
            return False
        if not self.does_sort():
            return self._simple_next(rcb, state)
        return self._sorting_next(rcb, state)

    def open(self, rcb):
        state = ReceiveIter.ReceiveIterState(self, rcb, self)
        rcb.set_state(self.state_pos, state)
        rcb.inc_memory_consumption(state.memory_consumption)
        qreq = rcb.get_request()
        assert qreq.is_prepared()
        assert qreq.has_driver()

    def reset(self, rcb):
        raise IllegalStateException('Should never be called')

    @staticmethod
    def add_scanner(sorted_scanners, scanner):
        inserted = False
        len_sorted_scanners = len(sorted_scanners)
        if len_sorted_scanners > 0:
            for index in range(len_sorted_scanners):
                if scanner.compare_to(sorted_scanners[index]) == -1:
                    sorted_scanners.insert(index, scanner)
                    inserted = True
                    break
        if not inserted:
            sorted_scanners.append(scanner)

    def _check_duplicate(self, rcb, state, res):
        if self._prim_key_fields is None:
            return False
        bin_prim_key = self._create_binary_primkey(res)
        if bin_prim_key in state.prim_keys_set:
            if rcb.get_trace_level() >= 1:
                rcb.trace(
                    'ReceiveIter._check_duplicate() : result was duplicate')
            return True
        else:
            state.prim_keys_set.append(bin_prim_key)
        sz = self.sizeof(bin_prim_key)
        state.memory_consumption += sz
        state.dup_elim_memory += sz
        rcb.inc_memory_consumption(sz)
        return False

    def _create_binary_primkey(self, result):
        binary_primkey = bytearray()
        out = ByteOutputStream(binary_primkey)
        for i in range(len(self._prim_key_fields)):
            fval = result.get(self._prim_key_fields[i])
            self._write_value(out, fval, i)
        return binary_primkey

    def _handle_topology_change(self, rcb, state):
        new_topo_info = rcb.get_topology_info()
        if ((self.distribution_kind ==
             ReceiveIter.DISTRIBUTION_KIND.ALL_PARTITIONS) or
                new_topo_info == state.topo_info):
            return
        new_shards = new_topo_info.get_shard_ids()
        curr_shards = state.topo_info.get_shard_ids()
        for i in range(len(new_shards)):
            equal = False
            for j in range(len(curr_shards)):
                if new_shards[i] == curr_shards[j]:
                    curr_shards[j] = -1
                    equal = True
                    break
            if equal:
                continue
            # We have a new shard
            ReceiveIter.add_scanner(
                state.sorted_scanners,
                ReceiveIter.RemoteScanner(
                    self, rcb, state, True, new_shards[i]))
        for j in range(len(curr_shards)):
            if curr_shards[j] == -1:
                continue
            # This shard does not exist any more
            for scanner in state.sorted_scanners:
                if scanner.shard_or_part_id == curr_shards[j]:
                    state.sorted_scanners.remove(scanner)
                    break
        state.topo_info = new_topo_info

    def _init_partition_sort(self, rcb, state):
        """
        Make sure we receive (and cache) at least one result per partition
        (except from partitions that do not contain any results at all).
        """
        assert state.is_in_sort_phase1
        # Create and execute a request to get at least one result from the
        # partition whose id is specified in continuation_key and from any other
        # partition that is co-located with that partition.
        req = rcb.get_request().copy_internal()
        req.set_cont_key(state.continuation_key)
        if rcb.get_trace_level() >= 1:
            rcb.trace('ReceiveIter : executing remote request for sorting ' +
                      'phase 1.')
        result = rcb.get_client().execute(req)
        num_pids = result.get_num_pids()
        results = result.get_results_internal()
        state.is_in_sort_phase1 = result.is_in_phase1()
        state.continuation_key = result.get_continuation_key()
        rcb.tally_read_kb(result.get_read_kb())
        rcb.tally_read_units(result.get_read_units())
        rcb.tally_write_kb(result.get_write_kb())
        if rcb.get_trace_level() >= 1:
            rcb.trace('ReceiveIter._init_partition_sort() : got result\n' +
                      'reached limit = ' + str(result.reached_limit()) +
                      ' in phase 1 = ' + str(result.is_in_phase1()))
        # For each partition P that was accessed during the execution of the
        # above QueryRequest, collect the results for P and create a scanner
        # that will be used during phase 2 to collect further results from P
        # only.
        res_idx = 0
        for p in range(num_pids):
            pid = result.get_pid(p)
            num_results = result.get_num_partition_results(p)
            cont_key = result.get_partition_cont_key(p)
            assert num_results > 0
            partition_results = list()
            for i in range(num_results):
                res = results[res_idx]
                partition_results.append(res)
                if rcb.get_trace_level() >= 1:
                    rcb.trace('Added result for partition ' + str(pid) + ':\n' +
                              str(res))
                res_idx += 1
            scanner = ReceiveIter.RemoteScanner(self, rcb, state, False, pid)
            scanner.add_results(partition_results, cont_key)
            ReceiveIter.add_scanner(state.sorted_scanners, scanner)
        if rcb.get_trace_level() >= 1:
            rcb.trace('ReceiveIter._init_partition_sort() : ' +
                      'memory consumption =  ' + str(state.memory_consumption))
        # For simplicity, if the size limit was not reached during this batch of
        # sort phase 1, we don't start a new batch. We let the app do it.
        # Furthermore, this means that each remote fetch will be done with the
        # max amount of read limit, which will reduce the total number of
        # fetches.
        rcb.set_reached_limit(True)

    def _simple_next(self, rcb, state):
        while True:
            res = state.scanner.next()
            if res is not None:
                if rcb.get_trace_level() >= 1:
                    rcb.trace('ReceiveIter._simple_next() : got result :\n' +
                              str(res))
                if self._check_duplicate(rcb, state, res):
                    continue
                rcb.set_reg_val(self.result_reg, res)
                return True
            break
        if rcb.get_trace_level() >= 1:
            rcb.trace('ReceiveIter._simple_next() : no result. Reached limit=' +
                      str(rcb.reached_limit()))
        if not rcb.reached_limit():
            state.done()
        return False

    def _sorting_next(self, rcb, state):
        if ((self.distribution_kind ==
             ReceiveIter.DISTRIBUTION_KIND.ALL_PARTITIONS) and
                state.is_in_sort_phase1):
            self._init_partition_sort(rcb, state)
            return False
        while True:
            try:
                scanner = state.sorted_scanners.pop(0)
            except IndexError:
                state.done()
                return False
            res = scanner.next_local()
            if res is not None:
                if rcb.get_trace_level() >= 1:
                    rcb.trace('ReceiveIter._sorting_next() : got result :\n' +
                              str(res))
                res = serde.BinaryProtocol.convert_value_to_none(res)
                rcb.set_reg_val(self.result_reg, res)
                if not scanner.is_done():
                    ReceiveIter.add_scanner(state.sorted_scanners, scanner)
                else:
                    if rcb.get_trace_level() >= 1:
                        rcb.trace(
                            'ReceiveIter._sorting_next() : done with ' +
                            'partition/shard ' + str(scanner.shard_or_part_id))
                if self._check_duplicate(rcb, state, res):
                    continue
                return True
            # Scanner had no cached results. If it may have remote results, send
            # a request to fetch more results. Otherwise, throw it away (by
            # leaving it outside sorted_scanners) and continue with another
            # scanner.
            if not scanner.is_done():
                try:
                    scanner.fetch()
                except RetryableException as e:
                    ReceiveIter.add_scanner(state.sorted_scanners, scanner)
                    raise e
            else:
                continue
            # We executed a remote fetch. If we got any result or the scanner
            # may have more remote results, put the scanner back into
            # sorted_scanner. Otherwise, throw it away.
            if not scanner.is_done():
                ReceiveIter.add_scanner(state.sorted_scanners, scanner)
            else:
                if rcb.get_trace_level() >= 1:
                    rcb.trace(
                        'ReceiveIter._sorting_next() : done with ' +
                        'partition/shard ' + str(scanner.shard_or_part_id))
            self._handle_topology_change(rcb, state)
            # For simplicity, we don't want to allow the possibility of another
            # remote fetch during the same batch, so whether or not the batch
            # limit was reached during the above fetch, we set limit flag to
            # True and return False, thus terminating the current batch.
            rcb.set_reached_limit(True)
            return False

    @staticmethod
    def _write_value(out, value, i):
        if isinstance(value, float):
            out.write_float(value)
        elif CheckValue.is_int(value):
            serde.BinaryProtocol.write_packed_int(out, value)
        elif CheckValue.is_long(value):
            serde.BinaryProtocol.write_packed_long(out, value)
        elif CheckValue.is_str(value):
            serde.BinaryProtocol.write_string(out, value)
        elif isinstance(value, datetime):
            serde.BinaryProtocol.write_datetime(out, value)
        elif isinstance(value, Decimal):
            serde.BinaryProtocol.write_decimal(out, value)
        else:
            raise QueryStateException(
                'Unexpected type for primary key column : ' + str(type(value)) +
                ', at result column ' + str(i))

    class ReceiveIterState(PlanIterState):

        def __init__(self, out, rcb, op_iter):
            super(ReceiveIter.ReceiveIterState, self).__init__()
            # The continuation key to be used for the next batch request during
            # sort-phase-1 of a sorting, all-partition query.
            self.continuation_key = None
            # The memory consumed for duplicate elimination.
            self.dup_elim_memory = 0
            # Used for sorting all-partition queries. It specifies whether the
            # query execution is in sort phase 1.
            self.is_in_sort_phase1 = True
            # The memory consumed by this ReceiveIter. Memory consumption is
            # counted for sorting all-partition queries and/or queries that do
            # duplicate elimination. We count the memory taken by results cached
            # in self.sorted_scanners and/or primary keys stored in
            # self.prim_keys_set.
            self.memory_consumption = 0
            # The remote scanner used for non-sorting queries.
            self.scanner = None
            # The remote scanners used for sorting queries. For all-shard
            # queries there is one RemoteScanner per shard. For all-partition
            # queries a RemoteScanner is created for each partition that has at
            # least one result.
            self.sorted_scanners = None
            # total_results_size and total_num_results store the total size and
            # number of results fetched by this ReceiveIter so far. They are
            # used to compute the average result size, which is then used to
            # compute the max number of results to fetch from a partition during
            # a sort-phase-2 request for a sorting, all-partition query.
            self.total_num_results = 0
            self.total_results_size = 0
            # It stores the set of shard ids. Needed for sorting all-shard
            # queries only.
            self.topo_info = rcb.get_topology_info()
            # The prim_keys_set is the hash set used for duplicate elimination.
            # It stores the primary keys (in binary format) of all the results
            # seen so far.
            if op_iter.does_dup_elim():
                self.prim_keys_set = list()
            else:
                self.prim_keys_set = None
            if (op_iter.does_sort() and
                    (op_iter.distribution_kind ==
                     ReceiveIter.DISTRIBUTION_KIND.ALL_PARTITIONS)):
                self.sorted_scanners = list()
            elif (op_iter.does_sort() and
                  (op_iter.distribution_kind ==
                   ReceiveIter.DISTRIBUTION_KIND.ALL_SHARDS)):
                num_shards = self.topo_info.num_shards()
                self.sorted_scanners = list()
                for i in range(num_shards):
                    ReceiveIter.add_scanner(
                        self.sorted_scanners,
                        ReceiveIter.RemoteScanner(
                            out, rcb, self, True,
                            self.topo_info.get_shard_id(i)))
            else:
                self.scanner = ReceiveIter.RemoteScanner(
                    out, rcb, self, False, -1)

        def clear(self):
            if self.prim_keys_set is not None:
                del self.prim_keys_set[:]
            if self.sorted_scanners is not None:
                del self.sorted_scanners[:]

        def close(self):
            super(ReceiveIter.ReceiveIterState, self).close()
            self.prim_keys_set = None
            self.sorted_scanners = None

        def done(self):
            super(ReceiveIter.ReceiveIterState, self).done()
            self.clear()

    class RemoteScanner(object):
        """
        For all-shard, ordering queries, there is one RemoteScanner per shard.
        In this case, each RemoteScanner will fetch results only from the shard
        specified by self.shard_or_part_id.

        For all-partition, ordering queries, there is one RemoteScanner for each
        partition that has at least one query result. In this case, each
        RemoteScanner will fetch results only from the partition specified by
        self.shard_or_part_id.

        For non-ordering queries, there is a single RemoteScanner. It will fetch
        as many as possible results starting from the shard or partition
        specified in self.continuation_key (so it may fetch results from more
        than one shard/partition).
        """

        def __init__(self, out, rcb, state, is_for_shard, spid):
            self._out = out
            self.rcb = rcb
            self.state = state
            self.is_for_shard = is_for_shard
            self.shard_or_part_id = spid
            self.results = None
            self.results_size = 0
            self.next_result_pos = 0
            self.continuation_key = None
            self.more_remote_results = True

        def add_results(self, results, cont_key):
            self.results = results
            self.continuation_key = cont_key
            self.more_remote_results = cont_key is not None
            self._add_memory_consumption()

        def compare_to(self, other):
            if not self.has_local_results():
                if not other.has_local_results():
                    return (-1 if self.shard_or_part_id < other.shard_or_part_id
                            else 1)
                return -1
            if not other.has_local_results():
                return 1
            v1 = self.results[self.next_result_pos]
            v2 = other.results[other.next_result_pos]
            comp = Compare.sort_results(self.rcb, v1, v2, self._out.sort_fields,
                                        self._out.sort_specs)
            if comp == 0:
                comp = (-1 if self.shard_or_part_id < other.shard_or_part_id
                        else 1)
            return comp

        def fetch(self):
            req = self.rcb.get_request().copy_internal()
            req.set_cont_key(self.continuation_key)
            req.set_shard_id(
                self.shard_or_part_id if self.is_for_shard else -1)
            if self._out.does_sort() and not self.is_for_shard:
                self.state.memory_consumption -= self.results_size
                self.rcb.dec_memory_consumption(self.results_size)
                num_results = ((req.get_max_memory_consumption() -
                                self.state.dup_elim_memory) //
                               ((len(self.state.sorted_scanners) + 1) *
                                (self.state.total_results_size //
                                 self.state.total_num_results)))
                if num_results > 2048:
                    num_results = 2048
                req.set_limit(int(num_results))
            if self.rcb.get_trace_level() >= 1:
                self.rcb.trace('RemoteScanner : executing remote request. '
                               'spid = ' + str(self.shard_or_part_id))
                assert req.has_driver()
            result = self.rcb.get_client().execute(req)
            self.results = result.get_results_internal()
            self.continuation_key = result.get_continuation_key()
            self.next_result_pos = 0
            self.more_remote_results = self.continuation_key is not None
            self.rcb.tally_read_kb(result.get_read_kb())
            self.rcb.tally_read_units(result.get_read_units())
            self.rcb.tally_write_kb(result.get_write_kb())
            assert result.reached_limit() or not self.more_remote_results
            # For simplicity, if the query is a sorting one, we consider the
            # current batch done as soon as we get the response back from the
            # proxy, even if the batch limit was not reached there.
            if result.reached_limit() or self._out.does_sort():
                self.rcb.set_reached_limit(True)
            if self._out.does_sort() and not self.is_for_shard:
                self._add_memory_consumption()
            if self.rcb.get_trace_level() >= 1:
                self.rcb.trace(
                    'RemoteScanner : got ' + str(len(self.results)) +
                    ' remote results. More remote results = ' +
                    str(self.more_remote_results) +
                    ' reached limit = ' + str(result.reached_limit()) +
                    ' read KB = ' + str(result.get_read_kb()) +
                    ' read Units = ' + str(result.get_read_units()) +
                    ' write KB = ' + str(result.get_write_kb()) +
                    ' memory consumption = ' +
                    str(self.state.memory_consumption))

        def has_local_results(self):
            return (self.results is not None and
                    self.next_result_pos < len(self.results))

        def is_done(self):
            return (not self.more_remote_results and
                    (self.results is None or
                     self.next_result_pos >= len(self.results)))

        def next(self):
            if (self.results is not None and
                    self.next_result_pos < len(self.results)):
                res = self.results[self.next_result_pos]
                self.next_result_pos += 1
                return res
            self.results = None
            self.next_result_pos = 0
            if not self.more_remote_results or self.rcb.reached_limit():
                return None
            self.fetch()
            assert self.results is not None
            if len(self.results) == 0:
                return None
            res = self.results[self.next_result_pos]
            self.next_result_pos += 1
            return res

        def next_local(self):
            if (self.results is not None and
                    self.next_result_pos < len(self.results)):
                res = self.results[self.next_result_pos]
                self.results[self.next_result_pos] = None
                self.next_result_pos += 1
                return res
            return None

        def _add_memory_consumption(self):
            self.results_size = 0
            for res in self.results:
                self.results_size += self._out.sizeof(res)
            self.state.total_num_results += len(self.results)
            self.state.total_results_size += self.results_size
            self.state.memory_consumption += self.results_size
            self.rcb.inc_memory_consumption(self.results_size)


class SFWIter(PlanIter):
    """
    SFWIter is used for:

    (a) project out result columns that do not appear in the SELECT list of
    the query, but are included in the results fetched from the proxy, because
    they are order-by columns or primary-key columns used for duplicate
    elimination.

    (b) For group-by and aggregation queries, regroup and reaggregate the
    partial groups/aggregates received from the proxy.

    (c) implement offset and limit.
    """

    def __init__(self, bis):
        super(SFWIter, self).__init__(bis)
        self._column_names = serde.BinaryProtocol.read_string_array(bis)
        self._num_gb_columns = bis.read_int()
        self._from_var_name = serde.BinaryProtocol.read_string(bis)
        self._is_select_star = bis.read_boolean()
        self.column_iters = self.deserialize_iters(bis)
        self._from_iter = self.deserialize_iter(bis)
        self._offset_iter = self.deserialize_iter(bis)
        self._limit_iter = self.deserialize_iter(bis)

    def close(self, rcb):
        state = rcb.get_state(self.state_pos)
        if state is None:
            return
        self._from_iter.close(rcb)
        for column_iter in self.column_iters:
            column_iter.close(rcb)
        if self._offset_iter is not None:
            self._offset_iter.close(rcb)
        if self._limit_iter is not None:
            self._limit_iter.close(rcb)
        state.close()

    def display_content(self, output, formatter):
        output = formatter.indent(output)
        output += 'FROM:\n'
        output = self._from_iter.display(output, formatter)
        output += ' as ' + self._from_var_name + '\n\n'
        if self._num_gb_columns >= 0:
            output = formatter.indent(output)
            output += 'GROUP BY:\n'
            output = formatter.indent(output)
            if self._num_gb_columns == 0:
                output += 'No grouping expressions'
            elif self._num_gb_columns == 1:
                output += 'Grouping by the first expression in the SELECT list'
            else:
                output += (
                        'Grouping by the first ' + str(self._num_gb_columns) +
                        'expressions in the SELECT list')
            output += '\n\n'
        output = formatter.indent(output)
        output += 'SELECT:\n'
        num_column_iters = len(self.column_iters)
        for i in range(num_column_iters):
            output = self.column_iters[i].display(output, formatter)
            if i < num_column_iters - 1:
                output += ',\n'
        if self._offset_iter is not None:
            output += '\n\n'
            output = formatter.indent(output)
            output += 'OFFSET:\n'
            output = self._offset_iter.display(output, formatter)
        if self._limit_iter is not None:
            output += '\n\n'
            output = formatter.indent(output)
            output += 'LIMIT:\n'
            output = self._limit_iter.display(output, formatter)
        return output

    def get_input_iter(self):
        return self._from_iter

    def get_kind(self):
        return PlanIter.PlanIterKind.value_of(ArithOpIter.PlanIterKind.SFW)

    def next(self, rcb):
        state = rcb.get_state(self.state_pos)
        if state.is_done():
            return False
        if state.num_results >= state.limit:
            state.done()
            return False
        # while loop for skipping offset results.
        while True:
            more = self._compute_next_result(rcb, state)
            if not more:
                return False
            """
            Even though we have a result, the state may be DONE. This is the
            case when the result is the last group tuple in a grouping SFW. In
            this case, if we have not reached the offset yet, we should ignore
            this result and return False.
            """
            if state.is_done() and state.offset > 0:
                return False
            if state.offset == 0:
                state.num_results += 1
                break
            state.offset -= 1
        return True

    def open(self, rcb):
        state = SFWIter.SFWIterState(self)
        rcb.set_state(self.state_pos, state)
        self._from_iter.open(rcb)
        for column_iter in self.column_iters:
            column_iter.open(rcb)
        self._compute_offset_limit(rcb)

    def reset(self, rcb):
        self._from_iter.reset(rcb)
        for column_iter in self.column_iters:
            column_iter.reset(rcb)
        if self._offset_iter is not None:
            self._offset_iter.reset(rcb)
        if self._limit_iter is not None:
            self._limit_iter.reset(rcb)
        state = rcb.get_state(self.state_pos)
        state.reset()
        self._compute_offset_limit(rcb)

    def _compute_next_result(self, rcb, state):
        # while loop for group by.
        while True:
            more = self._from_iter.next(rcb)
            if not more:
                if not rcb.reached_limit():
                    state.done()
                if self._num_gb_columns >= 0:
                    return self._produce_last_group(rcb, state)
                return False
            """
            Compute the expressions in the SELECT list. If this is a grouping
            SFW, compute only the group-by columns. However, skip this
            computation if this is not a grouping SFW and it has an offset that
            has not been reached yet.
            """
            if self._num_gb_columns < 0 < state.offset:
                return True
            num_cols = (self._num_gb_columns if self._num_gb_columns >= 0 else
                        len(self.column_iters))
            done = False
            for i in range(num_cols):
                column_iter = self.column_iters[i]
                more = column_iter.next(rcb)
                if not more:
                    if self._num_gb_columns > 0:
                        column_iter.reset(rcb)
                        done = True
                        break
                    rcb.set_reg_val(column_iter.get_result_reg(), None)
                else:
                    if rcb.get_trace_level() >= 3:
                        rcb.trace(
                            'SFW: Value for SFW column ' + str(i) + ' = ' +
                            str(rcb.ge_reg_val(column_iter.get_result_reg())))
                column_iter.reset(rcb)
            if done:
                continue
            if self._num_gb_columns < 0:
                if self._is_select_star:
                    break
                result = dict()
                rcb.set_reg_val(self.result_reg, result)
                for i in range(len(self.column_iters)):
                    column_iter = self.column_iters[i]
                    value = rcb.get_reg_val(column_iter.get_result_reg())
                    result[self._column_names[i]] = value
                break
            if self._group_input_tuple(rcb, state):
                break
        return True

    def _compute_offset_limit(self, rcb):
        state = rcb.get_state(self.state_pos)
        offset = 0
        limit = -1
        if self._offset_iter is not None:
            self._offset_iter.open(rcb)
            self._offset_iter.next(rcb)
            offset = rcb.get_reg_val(self._offset_iter.get_result_reg())
            if offset < 0:
                raise QueryException('Offset can not be a negative number: ' +
                                     str(self._offset_iter.location))
            if offset > pow(2, 31) - 1:
                raise QueryException(
                    'Offset can not be greater than 2^31 - 1: ' +
                    str(self._offset_iter.location))
        if self._limit_iter is not None:
            self._limit_iter.open(rcb)
            self._limit_iter.next(rcb)
            limit = rcb.get_reg_val(self._limit_iter.get_result_reg())
            if limit < 0:
                raise QueryException('Limit can not be a negative number: ' +
                                     str(self._limit_iter.location))
            if limit > pow(2, 31) - 1:
                raise QueryException(
                    'Limit can not be greater than 2^31 - 1: ' +
                    str(self._limit_iter.location))
        if limit < 0:
            limit = pow(2, 63) - 1
        state.offset = offset
        state.limit = limit

    def _group_input_tuple(self, rcb, state):
        """
        This method checks whether the current input tuple (a) starts the first
        group, i.e. it is the very 1st tuple in the input stream, or (b) belongs
        to the current group, or (c) starts a new group otherwise. The method
        returns True in case (c), indicating that an output tuple is ready to be
        returned to the consumer of this SFW. Otherwise, False is returned.
        """
        num_cols = len(self.column_iters)
        # If this is the very first input tuple, start the first group and go
        # back to compute next input tuple.
        if not state.have_gb_tuple:
            for i in range(self._num_gb_columns):
                state.gb_tuple[i] = rcb.get_reg_val(
                    self.column_iters[i].get_result_reg())
            for i in range(self._num_gb_columns, num_cols):
                self.column_iters[i].next(rcb)
                self.column_iters[i].reset(rcb)
            state.have_gb_tuple = True
            if rcb.get_trace_level() >= 2:
                rcb.trace('SFW: Started first group:')
                self._trace_current_group(rcb, state)
            return False
        # Compare the current input tuple with the current group tuple.
        equal = True
        for j in range(self._num_gb_columns):
            newval = rcb.get_reg_val(self.column_iters[j].get_result_reg())
            curval = state.gb_tuple[j]
            if newval != curval:
                equal = False
                break
        # If the input tuple is in current group, update the aggregate functions
        # and go back to compute the next input tuple.
        if equal:
            if rcb.get_trace_level() >= 2:
                rcb.trace('SFW: Input tuple belongs to current group:')
                self._trace_current_group(rcb, state)
            for i in range(self._num_gb_columns, num_cols):
                self.column_iters[i].next(rcb)
                self.column_iters[i].reset(rcb)
            return False

        # Input tuple starts new group. We must finish up the current group,
        # produce a result (output tuple) from it, and init the new group.

        # 1. Get the final aggregate values for the current group and store them
        # in gb_tuple.
        for i in range(self._num_gb_columns, num_cols):
            state.gb_tuple[i] = self.column_iters[i].get_aggr_value(
                rcb, True)

        # 2. Create a result dict out of the GB tuple.
        result = dict()
        rcb.set_reg_val(self.result_reg, result)
        for i in range(len(self.column_iters)):
            result[self._column_names[i]] = state.gb_tuple[i]
        if rcb.get_trace_level() >= 2:
            rcb.trace('SFW: Current group done: ' + str(result))

        # 3. Put the values of the grouping columns into the GB tuple.
        for i in range(self._num_gb_columns):
            column_iter = self.column_iters[i]
            state.gb_tuple[i] = rcb.get_reg_val(column_iter.get_result_reg())

        # 4. Compute the values of the aggregate functions.
        for i in range(self._num_gb_columns, num_cols):
            self.column_iters[i].next(rcb)
            self.column_iters[i].reset(rcb)
        if rcb.get_trace_level() >= 2:
            rcb.trace('SFW: Started new group:')
            self._trace_current_group(rcb, state)
        return True

    def _produce_last_group(self, rcb, state):
        if rcb.reached_limit():
            return False
        # If there is no group, return False.
        if not state.have_gb_tuple:
            return False
        result = dict()
        rcb.set_reg_val(self.result_reg, result)
        for i in range(self._num_gb_columns):
            result[self._column_names[i]] = state.gb_tuple[i]
        for i in range(self._num_gb_columns, len(self.column_iters)):
            result[self._column_names[i]] = (
                self.column_iters[i].get_aggr_value(rcb, True))
        if rcb.get_trace_level() >= 2:
            rcb.trace('SFW: Produced last group : ' + str(result))
        return True

    def _trace_current_group(self, rcb, state):
        for i in range(self._num_gb_columns):
            rcb.trace('SFW: Val ' + str(i) + ' = ' + state.gb_tuple[i])
        for i in range(self._num_gb_columns, len(self.column_iters)):
            rcb.trace('SFW: Val ' + str(i) + ' = ' +
                      str(self.column_iters[i].get_aggr_value(rcb, False)))

    class SFWIterState(PlanIterState):

        def __init__(self, op_iter):
            super(SFWIter.SFWIterState, self).__init__()
            self.offset = 0
            self.limit = 0
            self.num_results = 0
            self.gb_tuple = [0] * len(op_iter.column_iters)
            self.have_gb_tuple = False

        def reset(self):
            super(SFWIter.SFWIterState, self).reset()
            self.num_results = 0
            self.have_gb_tuple = False


class SortIter(PlanIter):
    """
    Sorts dict based on their values on a specified set of top-level fields.
    It is used by the driver to implement the geo_near function, which sorts
    results by distance.
    """

    def __init__(self, bis, kind):
        super(SortIter, self).__init__(bis)
        self._input = self.deserialize_iter(bis)
        self._sort_fields = serde.BinaryProtocol.read_string_array(bis)
        self._sort_specs = PlanIter.read_sort_specs(bis)
        if kind == PlanIter.PlanIterKind.SORT2:
            self._count_memory = bis.read_boolean()
        else:
            self._count_memory = True

    def close(self, rcb):
        state = rcb.get_state(self.state_pos)
        if state is None:
            return
        self._input.close(rcb)
        state.close()

    def display_content(self, output, formatter):
        output = self._input.display(output, formatter)
        output = formatter.indent(output)
        output += 'Sort Fields : '
        num_sort_fields = len(self._sort_fields)
        for i in range(num_sort_fields):
            output += self._sort_fields[i]
            if i < num_sort_fields - 1:
                output += ', '
        output += ',\n'
        return output

    def get_input_iter(self):
        return self._input

    def get_kind(self):
        return PlanIter.PlanIterKind.value_of(ArithOpIter.PlanIterKind.SORT)

    def next(self, rcb):
        state = rcb.get_state(self.state_pos)
        if state.is_done():
            return False
        if state.is_open():
            more = self._input.next(rcb)
            while more:
                val = rcb.get_reg_val(self._input.get_result_reg())
                for field in self._sort_fields:
                    fval = val[field]
                    if isinstance(fval, dict) or isinstance(fval, list):
                        raise QueryException(
                            'Sort expression does not return a single atomic ' +
                            ' value', self.location)
                self._add_result(state.results, val, rcb)
                if self._count_memory:
                    rcb.inc_memory_consumption(self.sizeof(val))
                more = self._input.next(rcb)
            if rcb.reached_limit():
                return False
            state.set_state(PlanIterState.STATE.RUNNING)
        if state.curr_result < len(state.results):
            val = serde.BinaryProtocol.convert_value_to_none(
                state.results[state.curr_result])
            rcb.set_reg_val(self.result_reg, val)
            state.results[state.curr_result] = None
            state.curr_result += 1
            return True
        state.done()
        return False

    def open(self, rcb):
        state = SortIter.SortIterState()
        rcb.set_state(self.state_pos, state)
        self._input.open(rcb)

    def reset(self, rcb):
        self._input.reset(rcb)
        state = rcb.get_state(self.state_pos)
        state.reset()

    def _add_result(self, sorted_results, result, rcb):
        inserted = False
        len_sorted_results = len(sorted_results)
        if len_sorted_results > 0:
            for index in range(len_sorted_results):
                if Compare.sort_results(
                        rcb, result, sorted_results[index], self._sort_fields,
                        self._sort_specs) == -1:
                    sorted_results.insert(index, result)
                    inserted = True
                    break
        if not inserted:
            sorted_results.append(result)

    class SortIterState(PlanIterState):

        def __init__(self):
            super(SortIter.SortIterState, self).__init__()
            self.results = list()
            self.curr_result = 0

        def close(self):
            super(SortIter.SortIterState, self).close()
            del self.results[:]

        def done(self):
            super(SortIter.SortIterState, self).done()
            self.curr_result = 0
            del self.results[:]

        def reset(self):
            super(SortIter.SortIterState, self).reset()
            self.curr_result = 0
            del self.results[:]


class VarRefIter(PlanIter):
    """
    VarRefIter represents a reference to a non-external variable in the query.
    It simply returns the value that the variable is currently bound to. This
    value is computed by the variable's "domain iterator" (the iterator that
    evaluates the domain expression of the variable). The domain iterator stores
    the value in theResultReg of this VarRefIter.

    In the context of the driver, an implicit internal variable is used to
    represent the results arriving from the proxy. All other expressions that
    are computed at the driver operate on these results, so all such expressions
    reference this variable. This is analogous to the internal variable used in
    kvstore to represent the table alias in the FROM clause.

    self._name:
    The name of the variable. Used only when displaying the execution plan.
    """

    def __init__(self, bis):
        super(VarRefIter, self).__init__(bis)
        self._name = serde.BinaryProtocol.read_string(bis)

    def close(self, rcb):
        state = rcb.get_state(self.state_pos)
        if state is None:
            return
        state.close()

    def display(self, output=None, formatter=None):
        if output is None and formatter is None:
            output = ''
            formatter = QueryFormatter()
        output = formatter.indent(output)
        output = self.display_content(output, formatter)
        output = self.display_regs(output)
        return output

    def display_content(self, output, formatter):
        return output + 'VAR_REF(' + self._name + ')'

    def get_kind(self):
        return PlanIter.PlanIterKind.value_of(ArithOpIter.PlanIterKind.VAR_REF)

    def next(self, rcb):
        state = rcb.get_state(self.state_pos)
        if state.is_done():
            if rcb.get_trace_level() >= 4:
                rcb.trace('No Value for variable ' + self._name +
                          ' in register ' + str(self.result_reg))
            return False
        if rcb.get_trace_level() >= 4:
            rcb.trace('Value for variable ' + self._name + ' in register ' +
                      str(self.result_reg) + ':\n' +
                      str(rcb.get_reg_val(self.result_reg)))
        state.done()
        return True

    def open(self, rcb):
        rcb.set_state(self.state_pos, PlanIterState())

    def reset(self, rcb):
        state = rcb.get_state(self.state_pos)
        state.reset()


class Compare(object):

    @staticmethod
    def compare_atomics(rcb, v0, v1, for_sort=False):
        """
        Compare 2 atomic values.

        The method throws an exception if either of the 2 values is non-atomic
        or the values are not comparable. Otherwise, it returns 0 if v0 == v1,
        1 if v0 > v1, or -1 if v0 < v1.

        Whether the 2 values are comparable depends on the "forSort" parameter.
        If True, then values that would otherwise be considered non-comparable
        are assumed to have the following order:

        numerics < timestamps < strings < booleans < empty < JsonNone < None
        """
        if rcb.get_trace_level() >= 4:
            rcb.trace('Comparing values: \n' + str(v0) + '\n' + str(v1))
        if v0 is None:
            if v1 is None:
                return 0
            if for_sort:
                return 1
        elif isinstance(v0, JsonNone):
            if isinstance(v1, JsonNone):
                return 0
            if for_sort:
                return -1 if v1 is None else 1
        elif isinstance(v0, Empty):
            if isinstance(v1, Empty):
                return 0
            if for_sort:
                return -1 if v1 is None or isinstance(v1, JsonNone) else 1
        elif isinstance(v0, bool):
            if isinstance(v1, bool):
                return -1 if v0 < v1 else (0 if v0 == v1 else 1)
            if for_sort:
                if (CheckValue.is_digit(v1) or isinstance(v1, datetime) or
                        isinstance(v1, str)):
                    return 1
                if (v1 is None or isinstance(v1, JsonNone) or
                        isinstance(v1, Empty)):
                    return -1
        elif CheckValue.is_str(v0):
            if CheckValue.is_str(v1):
                return -1 if v0 < v1 else (0 if v0 == v1 else 1)
            if for_sort:
                if CheckValue.is_digit(v1) or isinstance(v1, datetime):
                    return 1
                if (v1 is None or isinstance(v1, JsonNone) or
                        isinstance(v1, Empty) or isinstance(v1, bool)):
                    return -1
        elif isinstance(v0, datetime):
            if isinstance(v1, datetime):
                return -1 if v0 < v1 else (0 if v0 == v1 else 1)
            if for_sort:
                if CheckValue.is_digit(v1):
                    return 1
                if (v1 is None or isinstance(v1, JsonNone) or
                        isinstance(v1, Empty) or isinstance(v1, bool) or
                        isinstance(v1, str)):
                    return -1
        elif CheckValue.is_digit(v0):
            if CheckValue.is_digit(v1):
                return -1 if v0 < v1 else (0 if v0 == v1 else 1)
            if (for_sort and
                    (v1 is None or isinstance(v1, JsonNone) or isinstance(v1, Empty)
                     or isinstance(v1, bool) or isinstance(v1, str) or
                     isinstance(v1, datetime))):
                return -1
        raise QueryStateException(
            'Cannot compare value of type ' + str(type(v0)) +
            ' with value of type ' + str(type(v1)))

    @staticmethod
    def hashcode(value):
        if value is None:
            return maxvalue
        if isinstance(value, JsonNone):
            return -maxvalue - 1
        if isinstance(value, Empty):
            return 0
        if isinstance(value, bytearray) or isinstance(value, list):
            code = 1
            for val in value:
                code = 31 * code + Compare.hashcode(val)
            return code
        if isinstance(value, dict):
            code = 1
            for (k, v) in value.items():
                code = 31 * code + hash(k) + Compare.hashcode(v)
            return code
        return hash(value)

    @staticmethod
    def sort_atomics(rcb, v0, v1, sort_pos, sort_specs):
        if v0 is None:
            if v1 is None:
                return 0
            if isinstance(v1, Empty) or isinstance(v1, JsonNone):
                return -1 if sort_specs[sort_pos].is_desc else 1
            return -1 if sort_specs[sort_pos].nones_first else 1
        if v1 is None:
            if isinstance(v0, Empty) or isinstance(v0, JsonNone):
                return 1 if sort_specs[sort_pos].is_desc else -1
            return 1 if sort_specs[sort_pos].nones_first else -1
        if isinstance(v0, Empty):
            if isinstance(v1, Empty):
                return 0
            if isinstance(v1, JsonNone):
                return 1 if sort_specs[sort_pos].is_desc else -1
            return -1 if sort_specs[sort_pos].nones_first else 1
        if isinstance(v1, Empty):
            if isinstance(v0, JsonNone):
                return -1 if sort_specs[sort_pos].is_desc else 1
            return 1 if sort_specs[sort_pos].nones_first else -1
        if isinstance(v0, JsonNone):
            if isinstance(v1, JsonNone):
                return 0
            return -1 if sort_specs[sort_pos].nones_first else 1
        if isinstance(v1, JsonNone):
            return 1 if sort_specs[sort_pos].nones_first else -1
        comp = Compare.compare_atomics(rcb, v0, v1, True)
        return -comp if sort_specs[sort_pos].is_desc else comp

    @staticmethod
    def sort_results(rcb, r0, r1, sort_fields, sort_specs):
        for i in range(len(sort_fields)):
            v0 = r0.get(sort_fields[i])
            v1 = r1.get(sort_fields[i])
            comp = Compare.sort_atomics(rcb, v0, v1, i, sort_specs)
            if rcb.get_trace_level() >= 3:
                rcb.trace("Sort-Compared " + str(v0) + " with " + str(v1) +
                          " res = " + str(comp))
            if comp != 0:
                return comp
        return 0

    class CompResult(object):

        def __init__(self):
            self.comp = -1
            self.incompatible = False

        def __str__(self):
            return ('(comp, incompatible) = (' + str(self.comp) + ', ' +
                    str(self.incompatible) + ')')

        def clear(self):
            self.comp = -1
            self.incompatible = False


class QueryDriver(object):
    """
    Drives the execution of "advanced" queries at the driver and contains all
    the dynamic state needed for this execution. The state is preserved across
    the query requests submitted by the application (i.e., across batches).
    """
    QUERY_V2 = 2
    QUERY_V3 = 3
    QUERY_VERSION = QUERY_V3
    BATCH_SIZE = 100
    DUMMY_CONT_KEY = bytearray()

    def __init__(self, request):
        self._client = None
        self._request = request
        request.set_driver(self)
        self._continuation_key = None
        self._topology_info = None
        self._prep_cost = 0
        self._rcb = None
        # The max number of results the app will receive per NoSQLHandle.query()
        # invocation
        self._batch_size = (request.get_limit() if request.get_limit() > 0 else
                            QueryDriver.BATCH_SIZE)
        self._results = None
        self._error = None

    def close(self):
        self._request.get_prepared_statement().driver_plan().close(self._rcb)
        if self._results is not None:
            del self._results[:]
            self._results = None

    def compute(self, result):
        # Computes a batch of results and fills-in the given QueryResult.
        prep = self._request.get_prepared_statement()
        assert not prep.is_simple_query()
        assert self._request.get_driver() == self
        # If non-none, self._error stores a non-retriable exception thrown
        # during a previous batch. In this case, we just rethrow that exception
        if self._error is not None:
            raise self._error
        # self._results may be non-empty if a retryable exception was thrown
        # during a previous batch. In this case, self._results stores the
        # results computed before the exception was thrown, and in this batch we
        # just return what we have.
        if self._results is not None:
            self._set_query_result(result)
            return
        op_iter = prep.driver_plan()
        if self._rcb is None:
            self._rcb = RuntimeControlBlock(
                self, iter, prep.num_iterators(), prep.num_registers(),
                prep.get_variable_values())
            # Tally the compilation cost
            self._rcb.tally_read_kb(self._prep_cost)
            self._rcb.tally_read_units(self._prep_cost)
            op_iter.open(self._rcb)
        self._results = list()
        try:
            more = op_iter.next(self._rcb)
            i = 0
            while more:
                res = self._rcb.get_reg_val(op_iter.get_result_reg())
                if not isinstance(res, dict):
                    raise IllegalStateException(
                        'Query result is not a dict:\n' + str(res))
                self._results.append(res)
                if self._rcb.get_trace_level() >= 2:
                    self._rcb.trace('QueryDriver: got result : ' + str(res))
                i += 1
                if i == self._batch_size:
                    break
                more = op_iter.next(self._rcb)
        except Exception as e:
            # If it's not a retryable exception, save it so that we throw it
            # again if the app resubmits the QueryRequest.
            if not isinstance(e, RetryableException):
                self._error = NoSQLException(
                    'QueryRequest cannot be continued after throwing a ' +
                    'non-retryable exception in a previous execution. ' +
                    'Set the continuation key to none in order to execute ' +
                    'the query from the beginning', e)
                op_iter.close(self._rcb)
                del self._results[:]
                self._results = None
            raise e
        if not more:
            if self._rcb.reached_limit():
                self._continuation_key = QueryDriver.DUMMY_CONT_KEY
                self._rcb.set_reached_limit(False)
            else:
                assert op_iter.is_done(self._rcb)
                self._continuation_key = None
        else:
            self._continuation_key = QueryDriver.DUMMY_CONT_KEY
        self._set_query_result(result)
        self._request.set_cont_key(self._continuation_key)

    def get_client(self):
        return self._client

    def get_request(self):
        return self._request

    def get_shard_id(self, i):
        return self._topology_info.get_shard_id(i)

    def get_topology_info(self):
        return self._topology_info

    def num_shards(self):
        return self._topology_info.num_shards()

    def set_client(self, client):
        self._client = client

    def set_prep_cost(self, prep_cost):
        self._prep_cost = prep_cost

    def set_topology_info(self, topology_info):
        self._topology_info = topology_info

    def _set_query_result(self, result):
        result.set_results(self._results)
        result.set_continuation_key(self._continuation_key)
        result.set_read_kb(self._rcb.get_read_kb())
        result.set_read_units(self._rcb.get_read_units())
        result.set_write_kb(self._rcb.get_write_kb())
        self._results = None
        self._rcb.reset_kb_consumption()


class QueryFormatter(object):
    """
    A simple class to hold query expression and plan formatting information,
    such as indent level. A new instance of this class is passed to display()
    methods.

    self._indent:\n
    The current number of space chars to be printed as indentation when
    displaying the expression tree or the query execution plan.
    """

    def __init__(self, indent_increment=2):
        self._indent_increment = indent_increment
        self._indent = 0

    def dec_indent(self):
        self._indent -= self._indent_increment

    def get_indent(self):
        return self._indent

    def get_indent_increment(self):
        return self._indent_increment

    def inc_indent(self):
        self._indent += self._indent_increment

    def indent(self, output):
        for i in range(self._indent):
            output += ' '
        return output

    def set_indent(self, indent):
        self._indent = indent


class RuntimeControlBlock(object):
    """
    Stores all state of an executing query plan. There is a single RCB instance
    per query execution, and all iterators have access to that instance during
    te execution.
    """

    def __init__(self, driver, root_iter, num_iters, num_regs, external_vars):
        self._query_driver = driver
        self._root_iter = root_iter
        self._external_vars = external_vars
        self._iterator_states = [0] * num_iters
        self._registers = [0] * num_regs
        self._reached_limit = False
        self._read_kb = 0
        self._read_units = 0
        self._write_kb = 0
        self._memory_consumption = 0
        self._math_context = driver.get_request().get_math_context()
        setcontext(self._math_context)

    def dec_memory_consumption(self, v):
        self._memory_consumption -= v
        assert self._memory_consumption >= 0

    def get_client(self):
        return self._query_driver.get_client()

    def get_consistency(self):
        return self.get_request().get_consistency()

    def get_external_var(self, var_id):
        if self._external_vars is None:
            return None
        return self._external_vars[var_id]

    def get_external_vars(self):
        return self._external_vars

    def get_math_context(self):
        return self._math_context

    def get_max_memory_consumption(self):
        return self.get_request().get_max_memory_consumption()

    def get_max_read_kb(self):
        return self.get_request().get_max_read_kb()

    def get_read_kb(self):
        return self._read_kb

    def get_read_units(self):
        return self._read_units

    def get_registers(self):
        return self._registers

    def get_reg_val(self, reg_id):
        return self._registers[reg_id]

    def get_request(self):
        return self._query_driver.get_request()

    def get_root_iter(self):
        return self._root_iter

    def get_state(self, pos):
        return self._iterator_states[pos]

    def get_timeout(self):
        return self.get_request().get_timeout()

    def get_topology_info(self):
        return self._query_driver.get_topology_info()

    def get_trace_level(self):
        return self.get_request().get_trace_level()

    def get_write_kb(self):
        return self._write_kb

    def inc_memory_consumption(self, v):
        self._memory_consumption += v
        assert self._memory_consumption >= 0
        if self._memory_consumption > self.get_max_memory_consumption():
            raise QueryStateException(
                'Memory consumption at the client exceeded maximum ' +
                'allowed value ' + str(self.get_max_memory_consumption()))

    def reached_limit(self):
        return self._reached_limit

    def reset_kb_consumption(self):
        self._read_kb = 0
        self._read_units = 0
        self._write_kb = 0

    def set_reached_limit(self, value):
        self._reached_limit = value

    def set_reg_val(self, reg_id, value):
        self._registers[reg_id] = value

    def set_state(self, pos, state):
        self._iterator_states[pos] = state

    def tally_read_kb(self, nkb):
        self._read_kb += nkb

    def tally_read_units(self, nkb):
        self._read_units += nkb

    def tally_write_kb(self, nkb):
        self._write_kb += nkb

    @staticmethod
    def trace(msg):
        print('D-QUERY: ' + msg)


class SortSpec(object):
    """
    The order-by clause, for each sort expression allows for an optional
    'sort spec', which specifies the relative order of NULLs (less than or
    greater than all other values) and whether the values returned by the sort
    expr should be sorted in ascending or descending order.

    The SortSpec class stores these two pieces of information.
    """

    def __init__(self, bis):
        self.is_desc = bis.read_boolean()
        self.nones_first = bis.read_boolean()


class TopologyInfo(object):

    def __init__(self, seq_num, shard_ids):
        self._seq_num = seq_num
        self._shard_ids = shard_ids

    def get_seq_num(self):
        return self._seq_num

    def get_shard_id(self, i):
        return self._shard_ids[i]

    def get_shard_ids(self):
        return self._shard_ids

    def hash_code(self):
        return self._seq_num

    def num_shards(self):
        return len(self._shard_ids)
