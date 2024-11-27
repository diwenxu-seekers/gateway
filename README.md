# Project Gateway
This project aims to provide a core library to interact with multiple brokers and multiple clients.

## Installation

``` bash
python -m pip install gateway_lib-*.whl
```


## Gateway
A Gateway instance directly interacts with the electronic trading system provided by a designated broker.
Note that market data is outside the scope of this project.

### Non functional requirements
1. Defines a unified interface to operate brokerage account
2. Each Gateway instance represents a single brokerage account

### Functional requirements
1. Connectivity
    1. Connect to broker
    2. Disconnect from broker
    3. Monitor broker connection status
    4. Access broker connection status
2. Place Orders
    1. Market order
    2. Limit order
    3. Stop order
	4. Stop limit order
    5. Time in force
3. Modify open orders
4. Cancel open orders
5. Requests
	1. Open orders,
	2. Open positions,
	3. Current account info
	4. Historical executions
6. Events
	1. on_connection_update
	2. on_order_update
	3. on_execution
	4. on_error
	5. on_receive_open_orders
	6. on_receive_open_positions
	7. on_receive_account_info
	8. on_receive_historical_executions

### Example
``` python
# minimal example
import gateway_lib as GL

def on_events(src, event):
    print(event)

def main():
	gw = GL.GatewayFactory.create({
        'type: 'interactive_broker',
        'name': 'gw01',
        'host': 'ib_host',
        'port': 7496,
        'client_id': 123,
        'reconnect_max_retry': -1,
        'reconnect_interval_in_sec': 10})
	gw.events.on_error(on_events)
	gw.events.on_connection_update(on_events)
	gw.events.on_order_update(on_events)
	gw.events.on_execution(on_events)
    gw.events.on_account_info_update(on_events)
    gw.events.on_position_update(on_events)
	gw.connect()
	time.sleep(5)
	gw.disconnect()
```

## GatewayManager
A GatewayManager instance manages the lifetime of its Gateway instances and interfaces with multiple clients.

### Non functional requirements
1. Manages a finite number of Gateway instances
2. Non-blocking API

### Functional requirements
1. Create Gateway instance
2. Destroy Gateway instance
3. Handle incoming and outgoing messages

### Example
``` python
# minimal example
def on_events(event):
    print(event)

def main():
    with GL.GatewayManager(message_handler=SampleMessageProcessor(), event_handler=on_events) as oms:
        oms.send(dict(cid='ClientA',gid='ib01',cmd='connect',
            broker='IB',host=ib_host,port=ib_port,client_id=123))
        time.sleep(5)
```