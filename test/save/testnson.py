import base64
from borneo.nson import (Nson, JsonSerializer)
from borneo.common import ByteInputStream
#encoded_pair = b64encode(pair.encode()).decode()

nson_str='BgAAAL0AAAADgGUEf4BjBgAAABMAAAADgXJ1BICBcmsEgIF3awR/gHIGAAAAlAAAAASBbWQF/QGMO0CKcYF4cAV/gXJ2AbGs7QAFdywAIf4TmR4QpkO+tXsUw49hwJgAAAAAAAAAhQEDAAAAAQAAAAEAAAAAAAIkvYBsBgAAAEIAAAADgWlkBYCDbmFtZQeCRm9vg3RhZ3MAAAAAJQAAAAODdGFncweDcm9ja4N0YWdzB4RtZXRhbIN0YWdzB4JiYXI='

nson_bytes = base64.b64decode(nson_str)
bis = ByteInputStream(nson_bytes)
ser = JsonSerializer(pretty=True)
Nson.generate_events_from_nson(bis, ser)
