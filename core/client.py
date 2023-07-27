import json
import asyncio
import base64
import pickle

from core.utils import TCPMessage


class PetalsClient:
    """
    Represents a client for interacting with the Petals server.

    The PetalsClient sends messages to the Petals server using a TCP connection.
    It can send search queries to the server and receive responses.

    Attributes:
        host (str): The host address of the Petals server.
        port (int): The port number on which the Petals server is listening.
        retries (int): The number of retries to attempt in case of connection issues.
        timeout (float): The timeout for waiting for server responses.

    Methods:
        send_search_query(search_input: dict) -> list:
            Send a search query to the Petals server and receive a list of matching file paths.

        send_message(message: TCPMessage) -> any:
            Send a TCPMessage to the Petals server and receive the server's response.

        parse_response(format: str, response_string: str) -> any:
            Parse the server's response based on the specified format.

    Example:
        client = PetalsClient('127.0.0.1', 8888)

        # Creating a message with text format
        message = TCPMessage("message", "text", "Hello server!")
        asyncio.run(client.send_message(message))

        # Creating a message with JSON format
        message = TCPMessage("message", "json", json.dumps({"key": "value"}))
        asyncio.run(client.send_message(message))

        # Creating a message with pickle format
        message = TCPMessage("message", "text", base64.b64encode(pickle.dumps({"key": "value"})).decode())
        asyncio.run(client.send_message(message))

        # Sending a search query and receiving matching files
        search_input = {
            "bloom_source": "bloom",
            "files": "APAC_AUS_*",
            "query": {
                'condition': 'AND',
                'rules': [
                    {
                        'column': 'account_status',
                        'value': 'Inactive'
                    },
                    {
                        'column': 'account_type',
                        'value': 'Savings'
                    },
                    {
                        'column': 'loan_status',
                        'value': 'Current'
                    }
                ]
            }
        }
        search_message = TCPMessage("search", "json", json.dumps(search_input))
        matching_files = asyncio.run(client.send_message(search_message))
        for file_path in matching_files:
            print(file_path)
    """

    def __init__(self, host, port, retries=3, timeout=10):
        """
        Initialize a PetalsClient object with the specified host, port, retries, and timeout.

        Args:
            host (str): The host address of the Petals server.
            port (int): The port number on which the Petals server is listening.
            retries (int, optional): The number of retries to attempt in case of connection issues. Default is 3.
            timeout (float, optional): The timeout for waiting for server responses in seconds. Default is 10.
        """
        self.host = host
        self.port = port
        self.retries = retries
        self.timeout = timeout

    async def send_search_query(self, search_input):
        """
        Send a search query to the Petals server and receive a list of matching file paths.

        Args:
            search_input (dict): A dictionary containing the search query parameters.

        Returns:
            list: A list of matching file paths received from the server.

        Example:
            search_input = {
                "bloom_source": "bloom",
                "files": "APAC_AUS_*",
                "query": {
                    'condition': 'AND',
                    'rules': [
                        {
                            'column': 'account_status',
                            'value': 'Inactive'
                        },
                        {
                            'column': 'account_type',
                            'value': 'Savings'
                        },
                        {
                            'column': 'loan_status',
                            'value': 'Current'
                        }
                    ]
                }
            }
            matching_files = asyncio.run(client.send_search_query(search_input))
            for file_path in matching_files:
                print(file_path)
        """
        # Convert the search_input dictionary to a JSON string
        search_input_json = json.dumps(search_input)

        # Create a message in XML format
        message = TCPMessage("query", "json", search_input_json)

        # Send the message to the server and receive the response
        response = await self.send_message(message)

        # Response is a Python object (a list of file paths) decoded by parse_response
        matching_files = response

        return matching_files

    async def send_message(self, message):
        """
        Send a TCPMessage to the Petals server and receive the server's response.

        This method establishes a TCP connection to the Petals server, sends the message in XML format,
        and waits for the server to respond. It retries the connection if necessary.

        Args:
            message (TCPMessage): The TCPMessage object to be sent to the Petals server.

        Returns:
            any: The server's response parsed based on the message format.

        Raises:
            Exception: If the server does not respond after the specified number of retries.

        Example:
            # Creating a message with text format
            message = TCPMessage("message", "text", "Hello server!")
            asyncio.run(client.send_message(message))
        """
        xml_string = message.to_xml()
        for attempt in range(self.retries):
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(self.host, self.port),
                    timeout=self.timeout)

                writer.write(xml_string.encode())
                await writer.drain()

                buffer = ""
                while True:
                    chunk = await asyncio.wait_for(
                        reader.read(1024),
                        timeout=self.timeout)

                    if not chunk:
                        break
                    buffer += chunk.decode()
                    if buffer.endswith(f"</{message.cls}>"):
                        break

                response_string = buffer.replace(f"<{message.cls}>", "").replace(f"</{message.cls}>", "").strip()
                response = self.parse_response(message.format, response_string)

                print(f"Received from server: {response}")

                writer.close()
                await writer.wait_closed()

                return response

            except (asyncio.TimeoutError, ConnectionRefusedError) as e:
                print(f"Attempt {attempt + 1} failed. Retrying.")
                await asyncio.sleep(2 ** attempt)

        raise Exception("Server is not responding.")

    def parse_response(self, format, response_string):
        """
        Parse the server's response based on the specified format.

        Args:
            format (str): The format of the server's response ('text', 'json', or 'base64').
            response_string (str): The server's response string.

        Returns:
            any: The parsed server response based on the specified format.

        Raises:
            ValueError: If the specified format is not one of 'text', 'json', or 'base64'.

        Example:
            # Parse a JSON response
            json_response = '{"key": "value"}'
            parsed_response = parse_response('json', json_response)
            print(parsed_response)  # Output: {'key': 'value'}
        """
        if format == "text":
            return response_string
        elif format == "json":
            return json.loads(response_string)
        elif format == "base64":
            return base64.b64decode(response_string).decode()
        else:
            raise ValueError(f"Unexpected response format: {format}")


# Example usage:
if __name__ == "__main__":
    client = PetalsClient('127.0.0.1', 8888)

    # Creating a message with text format
    message = TCPMessage("message", "text", "Hello server!")
    asyncio.run(client.send_message(message))

    # Creating a message with JSON format
    message = TCPMessage("message", "json", json.dumps({"key": "value"}))
    asyncio.run(client.send_message(message))

    # Creating a message with pickle format
    message = TCPMessage("message", "text", base64.b64encode(pickle.dumps({"key": "value"})).decode())
    asyncio.run(client.send_message(message))

    search_input = {
        "bloom_source": "bloom",
        "files": "APAC_AUS_*",
        "query": {
            'condition': 'AND',
            'rules': [
                {
                    'column': 'account_status',
                    'value': 'Inactive'
                },
                {
                    'column': 'account_type',
                    'value': 'Savings'
                },
                {
                    'column': 'loan_status',
                    'value': 'Current'
                }
            ]
        }
    }

    # Creating a search message in JSON format
    search_message = TCPMessage("search", "json", json.dumps(search_input))
    matching_files = asyncio.run(client.send_message(search_message))

    # Print the list of matching files
    for file_path in matching_files:
        print(file_path)
