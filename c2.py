import json

import asyncio


class PetalsClient:
    def __init__(self, host, port, retries=3, timeout=60):
        self.host = host
        self.port = port
        self.retries = retries
        self.timeout = timeout

    async def send_message(self, store, query):
        # Prepare message in JSON format
        message = json.dumps({'store': store, 'query': query})

        for attempt in range(self.retries):
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(self.host, self.port),
                    timeout=self.timeout)

                # Send the message
                writer.write(message.encode())
                await writer.drain()

                buffer = ""
                while True:
                    chunk = await asyncio.wait_for(
                        reader.read(1024),
                        timeout=self.timeout)

                    if not chunk:
                        break
                    buffer += chunk.decode()

                print(f"Received from server: {buffer}")

                writer.close()
                await writer.wait_closed()

                return buffer

            except (asyncio.TimeoutError, ConnectionRefusedError) as e:
                print(f"Attempt {attempt + 1} failed. Retrying.")
                await asyncio.sleep(2 ** attempt)

        raise Exception("Server is not responding.")


if __name__ == "__main__":
    client = PetalsClient('127.0.0.1', 8888)

    search_input = {
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

    matching_files = asyncio.run(client.send_message('bloom', search_input))

    # Print the list of matching files
    for file_path in matching_files:
        print(file_path)
