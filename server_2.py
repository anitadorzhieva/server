import asyncio


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


error_msg = 'error\nwrong command\n\n'
ok_msg = 'ok\n'


def make_answer_for_one_metric(answ, metric, metric_list):
    for val, timestamp in metric_list:
        answ += f'{metric} {val} {timestamp}\n'
    return answ


def make_get_answer(metric_dict, metric):
    answ = str()
    answ += ok_msg

    if metric == '*':
        for metric, l in metric_dict.items():
            answ = make_answer_for_one_metric(answ, metric, l)
    else:
        answ = make_answer_for_one_metric(answ, metric, metric_dict.get(metric, []))

    answ += '\n'
    return answ


def make_put(metric_dict, metric, val, timestamp):
    if metric in metric_dict:
        for v, t in metric_dict[metric]:
            if t == timestamp:
                metric_dict[metric].remove((v, t))
                break

    metric_dict.setdefault(metric, []).append((float(val), int(timestamp)))


    for metric, metic_list in metric_dict.items():
        metric_dict[metric] = sorted(metic_list, key=lambda val_timestamp: val_timestamp[1])


def process_data(msg, metric_dict):
    if msg.find('\n') + 1 != len(msg):

        return error_msg

    parsed = msg.split()

    # GET
    if len(parsed) == 2:
        # get <key>\n
        if not parsed[0] == "get":
            return error_msg

        return make_get_answer(metric_dict, parsed[1])

    # PUT
    elif len(parsed) == 4:
        # put <key> <value> <timestamp>\n
        put = parsed[0]
        metric = parsed[1]
        val = parsed[2]
        timestamp = parsed[3]

        val, timestamp = float(val), int(timestamp)

        if not put == "put":

            return error_msg

        make_put(metric_dict, metric, val, timestamp)

        return ok_msg + '\n'
    else:
        return error_msg


metrics = dict()


class ClientServerProtocol(asyncio.Protocol):
    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        resp = process_data(data.decode(), metrics)
        self.transport.write(resp.encode())


if __name__ == '__main__':
    run_server('127.0.0.1', 8888)
