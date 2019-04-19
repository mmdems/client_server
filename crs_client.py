# import socket
# import time
#
#
# class ClientError(Exception):
#     def __init__(self):
#         Exception.__init__(self, 'wrong command')
#
#
# class Client:
#     def __init__(self, host, port, timeout=None):
#         self.host = host
#         self.port = port
#         self.timeout = timeout
#
#     def put(self, key, value, timestamp=time.time()):
#         with socket.create_connection((self.host, self.port), timeout=self.timeout) as client_socket:
#             message = 'put' + ' ' + key + ' ' + str(value) + ' ' + str(int(timestamp)) + '\n'
#             client_socket.sendall(message.encode())
#             response = client_socket.recv(4096).decode()
#         if response == 'error\nwrong command\n\n':
#             raise ClientError()
#
#     def get(self, key):
#         with socket.create_connection((self.host, self.port), timeout=self.timeout) as client_socket:
#             message = 'get' + ' ' + key + '\n'
#             client_socket.sendall(message.encode())
#             response = client_socket.recv(4096).decode()
#         if response == 'ok\n\n':
#             data = {}
#         elif response == 'error\nwrong command\n\n':
#             raise ClientError()
#         else:
#             data = {}
#             response = response.rstrip().split('\n')
#             response.remove(response[0])
#             for metric in response:
#                 key, value, timestamp = metric.split()[0], float(metric.split()[1]), int(metric.split()[2])
#                 if data.get(key) is None:
#                     data[key] = [(timestamp, value)]
#                 else:
#                     data[key].append((timestamp, value))
#             for key in data:
#                 data[key].sort()
#         return data


import socket
import time


class ClientError(Exception):
    """Общий класс исключений клиента"""
    pass


class ClientSocketError(ClientError):
    """Исключение, выбрасываемое клиентом при сетевой ошибке"""
    pass


class ClientProtocolError(ClientError):
    """Исключение, выбрасываемое клиентом при ошибке протокола"""
    pass


class Client:
    def __init__(self, host, port, timeout=None):
        # класс инкапсулирует создание сокета
        # создаем клиентский сокет, запоминаем объект socke.socket в self
        self.host = host
        self.port = port
        try:
            self.connection = socket.create_connection((host, port), timeout)
        except socket.error as err:
            raise ClientSocketError("error create connection", err)

    def _read(self):
        """Метод для чтения ответа сервера"""
        data = b""
        # накапливаем буфер, пока не встретим "\n\n" в конце команды
        while not data.endswith(b"\n\n"):
            try:
                data += self.connection.recv(1024)
            except socket.error as err:
                raise ClientSocketError("error recv data", err)

        # не забываем преобразовывать байты в объекты str для дальнейшей работы
        decoded_data = data.decode()

        status, payload = decoded_data.split("\n", 1)
        payload = payload.strip()

        # если получили ошибку - бросаем исключение ClientError
        if status == "error":
            raise ClientProtocolError(payload)

        return payload

    def put(self, key, value, timestamp=None):
        timestamp = timestamp or int(time.time())

        # отправляем запрос команды put
        try:
            self.connection.sendall(
                f"put {key} {value} {timestamp}\n".encode()
            )
        except socket.error as err:
            raise ClientSocketError("error send data", err)

        # разбираем ответ
        self._read()

    def get(self, key):
        # формируем и отправляем запрос команды get
        try:
            self.connection.sendall(
                f"get {key}\n".encode()
            )
        except socket.error as err:
            raise ClientSocketError("error send data", err)

        # читаем ответ
        payload = self._read()

        data = {}
        if payload == "":
            return data

        # разбираем ответ для команды get
        for row in payload.split("\n"):
            key, value, timestamp = row.split()
            if key not in data:
                data[key] = []
            data[key].append((int(timestamp), float(value)))

        return data

    def close(self):
        try:
            self.connection.close()
        except socket.error as err:
            raise ClientSocketError("error close connection", err)


def _main():
    # проверка работы клиента
    client = Client("127.0.0.1", 8888, timeout=5)
    client.put("test", 0.5, timestamp=1)
    client.put("test", 2.0, timestamp=2)
    client.put("test", 0.5, timestamp=3)
    client.put("load", 3, timestamp=4)
    client.put("load", 4, timestamp=5)
    print(client.get("*"))

    client.close()


if __name__ == "__main__":
    _main()
