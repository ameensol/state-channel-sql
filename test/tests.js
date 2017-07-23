var chai = require('chai');
var chaiSubset = require('chai-subset');
chai.use(chaiSubset);

let pgtools = require('pgtools');
let assert = require('chai').assert;

let m = require('../pg-machinomy');

let TEST_DB = {
  host: 'localhost',
  database: 'pg-machinomy-test',
};

let update = (...args) => Object.assign({}, ...args);

describe('PGMachinomy', () => {
  let pgm;

  before(async () => {
    // Ignore any errors here, they'll be checked when we create the DB
    let cxn = update(TEST_DB, { database: undefined });
    await pgtools.dropdb(cxn, TEST_DB.database).then(null, () => {});
    await pgtools.createdb(cxn, TEST_DB.database);

    pgm = new m.PGMachinomy(TEST_DB);
    let res = await pgm.setupDatabase();
  });

  beforeEach(async () => {
    stateUpdateSequenceNum = 1;
    await pgm._query(`
      truncate table state_updates;
      truncate table invalid_state_updates;
      truncate table channels;
      truncate table channel_events;
      truncate table invalid_channel_events;
    `);
  });

  after(() => {
    pgm && pgm.close();
  });

  it('Connect to Postgres', async () => {
    let pgVersion = await pgm.selftest();
    assert.isAbove(+pgVersion.pg_version, 90400);
  });

  let testChannel = {
    chain_id: 1,
    contract_id: 'contract_id-xxxxxxxxxxxxxxxxxxxxxxxxxxxx',
    channel_id: 'channel_id-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
  };

  let stateUpdateSequenceNum = null;
  let testStateUpdate = x => update(testChannel, {
    ts: 1234,

    amount: 1.23,
    sequence_num: stateUpdateSequenceNum++,
    signature: 'signature-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
  }, x || {});

  let expectedDbState = update(testChannel, {
    'id': 1,
    'amount': 1.23,
    'sequence_num': 1,
    'signature': 'signature-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
    'ts': '1969-12-31T19:20:34'
  });

  it('Check state update: simple', async () => {
    let res = await pgm.getStateUpdateStatus(testStateUpdate());
    assert.deepEqual(res, {
      'signature_valid': true,
      'is_latest': true,
      'dupe_status': 'distinct',
    });
  });

  it('Insert state update: valid', async () => {
    let res = await pgm.insertStateUpdate(testStateUpdate());

    assert.deepEqual(res, {
      'id': 1,
      'created': true,
      'status': {
        'signature_valid': true,
        'is_latest': true,
        'dupe_status': 'distinct',
      },
      'is_latest': true,
      'latest_state': expectedDbState,
      'channel_payment': 1.23,
      'channel_remaining_balance': null,
    });
  });

  it('Insert state update: invalid: bad data', async () => {
    let res = await pgm.insertStateUpdate(testStateUpdate({ 'contract_id': 'invalid' }));

    assert.deepEqual(res, {
      'error': true,
      'reason': 'invalid_state: value for domain mcy_eth_address violates check constraint "mcy_eth_address_check"',
      'status': {
        'signature_valid': true,
        'is_latest': true,
        'dupe_status': 'distinct',
      },
    });
  });

  it('Get latest state', async () => {
    await pgm.insertStateUpdate(testStateUpdate());

    assert.deepEqual(await pgm.getLatestState(testChannel), update(expectedDbState, { id: 2 }));

    await pgm.insertStateUpdate(testStateUpdate({ amount: 2.34, sequence_num: 2 }));
    assert.deepEqual(await pgm.getLatestState(testChannel), update(expectedDbState, {
      'id': 3,
      'amount': 2.34,
      'sequence_num': 2,
    }));

  });

  it('Get latest state: invalid args', () => {
    return pgm.getLatestState({ chain_id: 1, contract_id: null, channel_id: null })
      .then(assert.fail, (err) => {
        assert.include(err.toString(), 'contract_id must not be null')
      });
  });

  it('Create channel: new', async () => {
    let res = await pgm.insertChannel(update(didCreateChannelEvent, didCreateChannelEvent.fields));
    assert.containSubset(res, {
      created: true,
      channel: {
        status: 'CS_PENDING',
      },
    });
  });

  it('Create channel: existing', async () => {
    await pgm.insertChannel(update(didCreateChannelEvent, didCreateChannelEvent.fields));
    let res = await pgm.insertChannel(update(didCreateChannelEvent, didCreateChannelEvent.fields));
    assert.containSubset(res, {
      created: false,
      channel: {
        status: 'CS_PENDING',
      },
    });
  });

  it('Get channel state', async () => {
    let res;

    await pgm.insertChannelEvent(didCreateChannelEvent);
    await pgm.insertChannelEvent(update(didDepositChannelEvent, { fields: { value: 5 }}));
    await pgm.insertStateUpdate(testStateUpdate({ amount: 1.5 }));

    res = await pgm.getChannelState(testChannel)
    assert.containSubset(res, {
      channel: {
        status: 'CS_OPEN',
        value: 5,
      },
      latest_state: {
        amount: 1.5,
      },
      current_payment: 1.5,
      current_remaining_balance: 3.5,
    });

    await pgm.insertStateUpdate(testStateUpdate({ amount: 3.0 }));

    res = await pgm.getChannelState(testChannel);
    assert.containSubset(res, {
      channel: {
        status: 'CS_OPEN',
        value: 5,
      },
      latest_state: {
        amount: 3.0,
      },
      current_payment: 3.0,
      current_remaining_balance: 2.0,
    });

  });


  let mkChanEvent = (type, fields) => (update(testChannel, {
    ts: 1234,
    block: 'block_hash-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
    sender: 'sender-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
    type: type,
    fields: fields,
  }));

  let didCreateChannelEvent = mkChanEvent('DidCreateChannel', {
    sender: 'sender-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
    receiver: 'receiver-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
  });

  let didDepositChannelEvent = mkChanEvent('DidDeposit', {
    value: 3.45,
  });

  let didStartSettleEvent = mkChanEvent('DidStartSettle', {
    payment: 1.23,
  });

  let didSettleEvent = mkChanEvent('DidSettle', {
    payment: 2.0,
  });

  it('Channel event: create channel', async () => {
    let res = await pgm.insertChannelEvent(didCreateChannelEvent);
    assert.equal(res.status, 'CS_OPEN');
  });

  it('Channel event: channel exists', async () => {
    let res;

    res = await pgm.insertChannel(update(didCreateChannelEvent, didCreateChannelEvent.fields));
    assert.containSubset(res, {
      created: true,
      channel: {
        status: 'CS_PENDING',
      },
    });

    res = await pgm.insertChannelEvent(didCreateChannelEvent);
    assert.equal(res.status, 'CS_OPEN');
  });

  it('Channel event: full lifecycle', async () => {
    let res;

    res = await pgm.insertChannelEvent(didCreateChannelEvent);
    assert.equal(res.status, 'CS_OPEN');

    res = await pgm.insertChannelEvent(didDepositChannelEvent);
    assert.equal(res.status, 'CS_OPEN');

    res = await pgm.insertChannelEvent(didStartSettleEvent);
    assert.equal(res.status, 'CS_SETTLING');

    res = await pgm.insertChannelEvent(didSettleEvent);
    assert.equal(res.status, 'CS_SETTLED');
  });

  async function assertInvalidChannelEventLogged() {
    let res = await pgm.insertChannelEvent(didSettleEvent);
    assert.containSubset(res, {
      'error': true,
      'type': 'invalid_event_type',
    });

    res = await pgm._queryOne(`SELECT * FROM invalid_channel_events`);
    assert.containSubset(res, {
      'type': 'invalid_event_type',
      'blob': {
        'type': 'DidSettle',
      },
    });
  }

  it('Channel event: invalid events are logged without channel', async () => {
    await assertInvalidChannelEventLogged();
  });

  it('Channel event: invalid events are logged with channel', async () => {
    await pgm.insertChannel(update(didCreateChannelEvent, didCreateChannelEvent.fields));
    await assertInvalidChannelEventLogged();
  });

});
