import asyncio


class ClientServerProtocol(asyncio.Protocol):
    storage = {}

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        response = ClientServerProtocol.process_data(data.decode())
        self.transport.write(response)

    @staticmethod
    def process_data(received_cmd):
        cmd = received_cmd.rstrip().split()
        if cmd[0] == 'put':
            key, value, timestamp = cmd[1], cmd[2], cmd[3]
            if ClientServerProtocol.storage.get(key) is None:
                ClientServerProtocol.storage[key] = [(int(timestamp), float(value))]
            else:
                if int(timestamp) - ClientServerProtocol.storage[key][-1][0] < 1:
                    ClientServerProtocol.storage[key].remove(ClientServerProtocol.storage[key][-1])
                ClientServerProtocol.storage[key].append((int(timestamp), float(value)))
                ClientServerProtocol.storage[key].sort()
            response = 'ok\n\n'
        elif cmd[0] == 'get':
            if ClientServerProtocol.storage == {}:
                response = 'ok\n\n'
            else:
                if cmd[1] == '*':
                    response = 'ok\n'
                    for key in list(ClientServerProtocol.storage.keys()):
                        for tup in ClientServerProtocol.storage[key]:
                            response = response + key + ' ' + str(tup[1]) + ' ' + str(tup[0]) + '\n'
                    response = response + '\n'
                else:
                    key = cmd[1]
                    if ClientServerProtocol.storage.get(key) is None:
                        response = 'ok\n\n'
                    else:
                        response = 'ok\n'
                        for tup in ClientServerProtocol.storage[key]:
                            response = response + key + ' ' + str(tup[1]) + ' ' + str(tup[0]) + '\n'
                        response = response + '\n'
        else:
            response = 'error\nwrong command\n\n'
        return response.encode()


def run_server(host, port):
    loop = asyncio.get_event_loop()
    coro = loop.create_server(ClientServerProtocol, host, port)
    server = loop.run_until_complete(coro)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


run_server('127.0.0.1', 8888)
