import pytest
import logging
import time
import pathlib
import datetime

from gateway_lib.ib import ExecutionManager

class DummyExecution:
    def __init__(self, execId=''):
        self.execId = execId

def create_executionObj(execId=''):
    return {'execution': DummyExecution(execId)}

def create_commissionObj(execId=''):
    return {'execId': execId, 'commission':0, 'currency':'USD'}

updates = []
def collect(update):
    updates.append(update)

def test_init_contains_only_default_bucket():
    mgr = ExecutionManager(dispatch=collect)
    assert len(mgr._buckets) == 1
    assert -1 in mgr._buckets

def test_historical_request_create_bucket():
    mgr = ExecutionManager(dispatch=collect)
    numOfRequest = 0
    for i in range(10):
        numOfRequest += 1
        mgr.request_historical(i)
        assert len(mgr._buckets) == numOfRequest+1
        assert i in mgr._buckets

def test_historical_request_end_remove_bucket():
    mgr = ExecutionManager(dispatch=collect)
    reqId = 1
    mgr.request_historical(reqId)
    mgr.notify_historical_end(reqId)
    assert len(mgr._buckets) == 1
    assert reqId not in mgr._buckets

def test_clear_removes_all_but_default_bucket():
    mgr = ExecutionManager(dispatch=collect)
    for i in range(10):
        mgr.request_historical(i)
    assert -1 in mgr._buckets

def test_execution_update():
    mgr = ExecutionManager(dispatch=collect)
    reqId = -1
    numOfUpdate = 0

    for x in range(10):
        numOfUpdate += 1
        updates.clear()
        execObj = create_executionObj(x)
        commObj = create_commissionObj(x)

        mgr.update_execution(reqId, execObj)

        # No dispatch unless execution and commission are paired
        assert len(updates) == 0

        mgr.update_commission(commObj['execId'], commObj)

        assert len(updates) == 1
        assert updates[0]['execution'].execId == x
        assert updates[0]['is_historical'] == False
        assert updates[0]['commission'] == commObj['commission']
        assert updates[0]['currency'] == commObj['currency']
        assert len(mgr._commissions) == numOfUpdate
        assert len(mgr._buckets[reqId]._executions) == 0

def test_execution_update_sequenceReversed():
    mgr = ExecutionManager(dispatch=collect)
    reqId = -1
    numOfUpdate = 0

    for x in range(10):
        updates.clear()
        numOfUpdate += 1
        execObj = create_executionObj(x)

        # No dispatch unless execution and commission are paired
        assert len(updates) == 0

        commObj = create_commissionObj(x)

        mgr.update_commission(commObj['execId'], commObj)
        mgr.update_execution(reqId, execObj)

        assert len(updates) == 1
        assert updates[0]['execution'].execId == x
        assert updates[0]['is_historical'] == False
        assert updates[0]['commission'] == commObj['commission']
        assert updates[0]['currency'] == commObj['currency']
        assert len(mgr._commissions) == numOfUpdate
        assert len(mgr._buckets[reqId]._executions) == 0

def test_request_execution_update():
    mgr = ExecutionManager(dispatch=collect)
    reqId = 2
    numOfUpdate = 10
    updates.clear()

    mgr.request_historical(reqId)

    for x in range(numOfUpdate):
        execObj = create_executionObj(x)
        mgr.update_execution(reqId, execObj)

    for x in range(numOfUpdate):
        commObj = create_commissionObj(x)
        mgr.update_commission(commObj['execId'], commObj)

    # No dispatch unless `historical_end`
    assert len(updates) == 0

    mgr.notify_historical_end(reqId)

    assert len(updates) == numOfUpdate
    assert all(x['is_historical'] for x in updates)
    assert all(x['execution'].execId < numOfUpdate for x in updates)
    assert all('commission' in x for x in updates)
    assert all('currency' in x for x in updates)
    assert len(mgr._commissions) == numOfUpdate
    assert reqId not in mgr._buckets


def test_request_execution_update_sequenceReversed():
    mgr = ExecutionManager(dispatch=collect)
    reqId = 2
    numOfUpdate = 10
    updates.clear()

    mgr.request_historical(reqId)

    for x in range(numOfUpdate):
        commObj = create_commissionObj(x)
        mgr.update_commission(commObj['execId'], commObj)

    for x in range(numOfUpdate):
        execObj = create_executionObj(x)
        mgr.update_execution(reqId, execObj)

    # No dispatch unless `historical_end`
    assert len(updates) == 0

    mgr.notify_historical_end(reqId)
    assert len(updates) == numOfUpdate

    assert all(x['is_historical'] for x in updates)
    assert all(x['execution'].execId < numOfUpdate for x in updates)
    assert all('commission' in x for x in updates)
    assert all('currency' in x for x in updates)
    assert len(mgr._commissions) == numOfUpdate
    assert reqId not in mgr._buckets


def test_clear_remove_records():
    mgr = ExecutionManager(dispatch=collect)
    reqId = 2
    numOfUpdate = 10
    updates.clear()
    mgr.request_historical(reqId)
    for x in range(numOfUpdate):
        commObj = create_commissionObj(x)
        mgr.update_commission(commObj['execId'], commObj)
    for x in range(numOfUpdate):
        execObj = create_executionObj(x)
        mgr.update_execution(reqId, execObj)
    [mgr.update_execution(-1, create_executionObj(x+100)) for x in range(numOfUpdate)]
    assert len(mgr._buckets[reqId].events) == numOfUpdate
    assert len(mgr._buckets[-1]._executions) == numOfUpdate
    assert len(updates) == 0
    assert len(mgr._commissions) == numOfUpdate

    mgr.clear()

    assert len(mgr._commissions) == 0
    assert len(mgr._buckets) == 1
    assert -1 in mgr._buckets
    assert len(mgr._buckets[-1]._executions) == 0
    assert len(mgr._buckets[-1].events) == 0


def test_sharing_commission():
    mgr = ExecutionManager(dispatch=collect)
    reqId = 2
    mgr.request_historical(reqId)
    updates.clear()
    execId = 'a'

    mgr.update_execution(reqId, create_executionObj(execId))
    mgr.update_execution(-1, create_executionObj(execId))
    mgr.update_commission(execId, create_commissionObj(execId))

    assert len(updates) == 1
    assert updates[0]['execution'].execId == execId
    assert len(mgr._buckets[reqId]._executions) == 0
    assert len(mgr._buckets[reqId].events) == 1
    assert mgr._buckets[reqId].events[0]['execution'].execId == execId
    assert len(mgr._commissions) == 1