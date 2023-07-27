import base64
import functools
import inspect
import json
from xml.sax.saxutils import escape
import xml.etree.ElementTree as ET

from core.filters import Filter


def cache(func):
    cache = {}

    @functools.wraps(func)
    def wrapper(*args):
        if args not in cache:
            cache[args] = func(*args)
        return cache[args]

    return wrapper


def ensure_json_output(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        result = await func(*args, **kwargs)
        if isinstance(result, str):
            result = {"response": result}  # Convert non-dict strings to JSON format
        return json.dumps(result)

    return wrapper


@cache
def get_filter_classes():
    # Obtain all non-abstract subclasses of Filter
    subclasses = set(cls for cls in Filter.__subclasses__()
                     if not inspect.isabstract(cls))

    # Create a mapping from names to classes
    return {cls.name: cls for cls in subclasses}

class TCPMessage:

    def __init__(self, cls, message_format, payload):
        self.cls = cls
        self.format = message_format
        self.payload = payload

    def to_xml(self):
        root = ET.Element(self.cls)
        root.set("format", self.format)
        root.text = escape(self.payload)  # ensure that payload is properly escaped
        return ET.tostring(root, encoding="unicode")


def parse_message(xml_string):
    root = ET.fromstring(xml_string)

    cls = root.tag
    message_format = root.get("format")
    payload = root.text.strip()

    # Parse the payload based on its format
    if message_format == "json":
        payload = json.loads(payload)
    elif message_format == "base64":
        payload = base64.b64decode(payload).decode('utf-8')

    return TCPMessage(cls, message_format, payload)
