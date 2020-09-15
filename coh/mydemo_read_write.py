from ethvigil.EVCore import EVCore
import threading
from websocket_listener import consumer_contract
import queue
import asyncio
import time
import signal
import json


update_q = queue.Queue()
update_map = dict()


class EthVigilWSSubscriber(threading.Thread):
    def __init__(self, group=None, target=None, name=None, args=(), kwargs=None, daemon=None):
        super().__init__(group=group, target=target, name=name, daemon=daemon)
        self._args = args
        self._kwargs = kwargs
        self._api_read_key = self._kwargs['api_read_key']
        self._update_q = self._kwargs['update_q']
        self._ev_loop = self._kwargs['ev_loop']

        self.shutdown_flag = threading.Event()

    def run(self) -> None:
        asyncio.set_event_loop(self._ev_loop)
        asyncio.get_event_loop().run_until_complete(consumer_contract(self._api_read_key, self._update_q))
        while not self.shutdown_flag.is_set():
            time.sleep(1)
        # print('Stopping thread ', self.ident)


async def async_shutdown(signal, loop):
    # print(f'Received exit signal {signal.name}...')
    # print('Nacking outstanding messages')
    tasks = [t for t in asyncio.Task.all_tasks() if t is not
             asyncio.Task.current_task()]

    [task.cancel() for task in tasks]

    # print(f'Cancelling {len(tasks)} outstanding tasks')
    await asyncio.gather(*tasks)
    loop.stop()
    # print('Event loop Shutdown complete.')


def sync_shutdown(signum, frame):
    # print('Caught signal %d' % signum)
    raise ServiceExit

class ServiceExit(Exception):
    pass

def main(event_loop):
    evc = EVCore(verbose=False)
    api_read_key = evc._api_read_key
    t = EthVigilWSSubscriber(kwargs={
        'api_read_key': api_read_key,
        'update_q': update_q,
        'ev_loop': event_loop,
        'filter': 'contractmon'
    })
    t.start()
    contract_instance = evc.generate_contract_sdk(
        contract_address='0xab3b558a920ff2691f5d5923c474b02cf84ddfc3',
        app_name='myDemoContract'
    )
    auditlog_contract_instance = evc.generate_contract_sdk(
        contract_address='0xe7c9bfc1e19587de9aed19d6eb51697c7da3451b',
        app_name='myAuditLog'
    )
    try:
        while True:
            params = {'incrValue': 150, '_note': 'NewNote' + str(int(time.time())) }
            tx = contract_instance.setContractInformation(**params)[0]['txHash']
            print('Sending tx to setContractInformation with params: ', params)
            print('setContractInformation tx response: ', tx)
            p = update_q.get()
            p = json.loads(p)

            if p.get('type') == 'event' \
                    and p['event_name'] == 'ContractIncremented':
                print(p, '\n\n')
                print('Received setContractInformation event confirmation: ', tx)
                print('Writing to audit log contract...')
                audit_tx = auditlog_contract_instance.addAuditLog(
                    _newNote=p['event_data']['newNote'],
                    _changedBy=p['event_data']['incrementedBy'],
                    _incrementValue=p['event_data']['incrementedValue'],
                    _timestamp=p['ctime']
                )
                print('Wrote to audit log contract. Tx response: ', audit_tx[0]['txHash'])

            update_q.task_done()
            time.sleep(5)
    except ServiceExit:
        # print('main() received ServiceExit')
        t.shutdown_flag.set()
        t.join()


if __name__ == '__main__':
    main_loop = asyncio.get_event_loop()
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    # for s in signals:
    #     ev_loop.add_signal_handler(
    #         s, lambda s=s: asyncio.create_task(shutdown(s, ev_loop)))
    for s in signals:
        main_loop.add_signal_handler(
            s, lambda s=s: asyncio.get_event_loop().create_task(async_shutdown(s, main_loop)))
    for s in signals:
        signal.signal(s, sync_shutdown)
    try:
        main(main_loop)
    except ServiceExit:
        pass

