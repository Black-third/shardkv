# redis-py, with the hiredis parser: it is the default a modern application gets
# (redis-py recommends it) and it is the stricter of the two parsers, so a reply
# shape that only the pure-Python parser tolerates shows up as a failure here
# rather than in production.
FROM python:3.12-slim

RUN pip install --no-cache-dir --root-user-action=ignore "redis[hiredis]"

COPY compat.py /suite/compat.py
CMD ["python", "-u", "/suite/compat.py"]
