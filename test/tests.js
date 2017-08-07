var chai = require('chai');
var chaiSubset = require('chai-subset');
chai.use(chaiSubset);

let _pga = require('pg-async');
let [PgAsync, SQL] = [_pga['default'], _pga.SQL];

let assert = require('chai').assert;

let m = require('../pg-machinomy');

let TEST_DB = {
  host: 'localhost',
  database: 'pg-machinomy-test',
};

let update = (...args) => Object.assign({}, ...args);

let mkrpad = len => (prefix='') => {
  return prefix + '0'.repeat(len - prefix.length);
};

let mkhash = mkrpad(64);
let mkchanid = mkrpad(64);
let mkcontractid = mkrpad(40);
let mksig = mkrpad(130);

describe('PGMachinomy', () => {
  let pgm;

  let realValidateSignatureFunctions;

  before(async () => {
    // Ignore any errors here, they'll be checked when we create the DB
    let cxn = new PgAsync(update(TEST_DB, { database: 'postgres' }))
    await cxn.query(`DROP DATABASE "${TEST_DB.database}"`).then(null, () => {});
    await cxn.query(`CREATE DATABASE "${TEST_DB.database}"`);

    pgm = new m.PGMachinomy(TEST_DB);
    await pgm.setupDatabase();
    realValidateSignatureFunctions = await pgm._queryOne('res', SQL`
      select
        proname,
        array_agg(pg_get_functiondef(pp.oid)) as res
      from pg_proc pp
      inner join pg_namespace pn on (pp.pronamespace = pn.oid)
      inner join pg_language pl on (pp.prolang = pl.oid)
      where
        proname = 'ecdsa_verify'
      group by proname
    `);

    // Mock out ecdsa_verify so we don't need to worry about having valid
    // signatures during these tests (it will be put back in for the signature
    // tests when we need to do that).
    realValidateSignatureFunctions.forEach(async (func) => {
      let sig = func.toLowerCase().match(/ecdsa_verify\(.*\)/)[0];
      await pgm._query(`
        create or replace function ${sig} returns boolean
        language sql as $$ select true $$;
      `);
    });
  });

  beforeEach(async () => {
    stateUpdateSequenceNum = 1;
    await pgm._query(`
      truncate table state_updates cascade;
      truncate table invalid_state_updates cascade;
      truncate table channel_events cascade;
      truncate table channel_intents cascade;
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
    contract_id: mkcontractid('c'),
    channel_id: mkchanid('a'),
  };

  let stateUpdateSequenceNum = null;
  let testStateUpdate = x => update(testChannel, {
    ts: 1234,

    amount: 1.23,
    sequence_num: stateUpdateSequenceNum++,
    signature: mksig(),
  }, x || {});

  let expectedDbState = update(testChannel, {
    'amount': 1.23,
    'sequence_num': 1,
    'signature': mksig(),
  });

  it('Check state update: simple', async () => {
    let res = await pgm.getStateUpdateStatus(testStateUpdate());
    assert.containSubset(res, {
      'signature_valid': true,
      'is_latest': true,
      'dupe_status': 'distinct',
    });
  });

  it('Insert state update: valid', async () => {
    let res = await pgm.insertStateUpdate(testStateUpdate());

    assert.containSubset(res, {
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
    let res = await pgm.insertStateUpdate(testStateUpdate({ 'contract_id': '0000' }));

    assert.containSubset(res, {
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

    assert.containSubset(await pgm.getLatestState(testChannel), expectedDbState);

    await pgm.insertStateUpdate(testStateUpdate({ amount: 2.34, sequence_num: 2 }));
    assert.containSubset(await pgm.getLatestState(testChannel), update(expectedDbState, {
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

  let blockNum = 0;
  let mkChanEvent = (type, fields) => (update(testChannel, {
    ts: 1234,
    block_hash: mkhash('block_hash'),
    block_number: blockNum++,
    sender: 'sender-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
    event_type: type,
    fields: fields,
  }));

  let didCreateChannelEvent = mkChanEvent('DidCreateChannel', {
    sender: 'sender-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
    receiver: 'receiver-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
    settlement_period: 17,
    until: 7890,
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

  it('Get channel: empty', async () => {
    let res = await pgm.getChannelStatus(testChannel)
    assert.containSubset(res, {
      channel: null,
      latest_event: null,
      latest_intent_event: null,
      latest_chain_event: null,
    });
  });

  it('Get channel: with a few events', async () => {
    let res;

    await pgm.insertChannelEvent(didCreateChannelEvent);
    await pgm.insertChannelEvent(update(didDepositChannelEvent, { fields: { value: 5 }}));
    await pgm.insertStateUpdate(testStateUpdate({ amount: 1.5 }));

    res = await pgm.getChannelStatus(testChannel)
    assert.containSubset(res, {
      channel: {
        state: 'CS_OPEN',
        value: 5,
      },

      latest_chain_event: {
        event_type: 'DidDeposit',
      },

      latest_event: {
        event_type: 'DidDeposit',
      },

      latest_intent_event: null,

      latest_state: {
        amount: 1.5,
      },

      is_invalid: false,
      is_invalid_reason: null,

      current_payment: 1.5,
      current_remaining_balance: 3.5,
    });

    res = await pgm.insertStateUpdate(testStateUpdate({ amount: 3 }));
    assert.containSubset(res, {
      'channel_payment': 3,
      'channel_remaining_balance': 2,
    });

    await pgm.insertChannelIntent(update(didStartSettleEvent));

    res = await pgm.getChannelStatus(testChannel);
    assert.containSubset(res, {
      channel: {
        state: 'CS_SETTLING',
        value: 5,
      },

      latest_chain_event: {
        event_type: 'DidDeposit',
      },

      latest_event: {
        event_type: 'DidStartSettle',
      },

      latest_intent_event: {
        event_type: 'DidStartSettle',
      },

      latest_state: {
        amount: 3.0,
      },

      is_invalid: false,
      is_invalid_reason: null,

      current_payment: 3.0,
      current_remaining_balance: 2.0,
    });

  });

  it('Channel event: full lifecycle', async () => {
    let res;

    res = await pgm.insertChannelEvent(didCreateChannelEvent);
    assert.containSubset(res, {
      channel: {state: 'CS_OPEN'}
    });

    res = await pgm.insertChannelEvent(didDepositChannelEvent);
    assert.containSubset(res, {
      channel: {state: 'CS_OPEN'}
    });

    res = await pgm.insertChannelEvent(didStartSettleEvent);
    assert.containSubset(res, {
      channel: {state: 'CS_SETTLING'}
    });

    res = await pgm.insertChannelEvent(didSettleEvent);
    assert.containSubset(res, {
      channel: {state: 'CS_SETTLED'}
    });
  });

  it('Channel event: returned by mcy_get_channel_events', async () => {
    await pgm.insertChannelEvent(didCreateChannelEvent);
    await pgm.insertChannelIntent(didDepositChannelEvent);
    let rows = await pgm.getChannelEvents(testChannel);
    assert.containSubset(rows[0], {
      'event_type': 'DidCreateChannel',
    });
    assert.containSubset(rows[1], {
      event_type: 'DidDeposit',
      block_hash: null,
    });

    rows = await pgm.getChannelEvents(testChannel, false);
    assert.equal(rows.length, 1);
    assert.containSubset(rows[0], {
      'event_type': 'DidCreateChannel',
    });
  });

  it('Set recent blocks: simple', async () => {
    let insertEvent = (base, num, hash, fields) => {
      base = update(base, { fields: update(base.fields, fields) });
      return pgm.insertChannelEvent(update(base, {
        block_number: num,
        block_hash: mkhash(hash),
      }));
    };

    let res;

    await insertEvent(didCreateChannelEvent, 1, 'a');
    await insertEvent(didDepositChannelEvent, 2, 'b', { value: 1 });
    await insertEvent(didDepositChannelEvent, 3, 'c', { value: 2 });

    res = await pgm.getChannelStatus(testChannel);
    assert.containSubset(res, { channel: { value: 3 }});

    res = await pgm.setRecentBlocks(testChannel.chain_id, 1, ['a', 'b'].map(mkhash));
    assert.containSubset(res, {
      'updated_event_count': 1,
      'updated_channels': [
        { channel: { value: 1 }},
      ],
    });

    res = await pgm.setRecentBlocks(testChannel.chain_id, 1, ['a', 'x', 'c'].map(mkhash));
    assert.containSubset(res, {
      'updated_event_count': 2,
      'updated_channels': [
        { channel: { value: 2 }},
      ],
    });

  });

  it('Invalid events: two different senders create same channel', async () => {
    await pgm.insertChannelEvent(update(didCreateChannelEvent, {
      sender: 's1-' + 'x'.repeat(37),
    }));
    await pgm.insertChannelEvent(update(didCreateChannelEvent, {
      sender: 's2-' + 'x'.repeat(37),
    }));

    let res = await pgm.getChannelStatus(testChannel);
    assert.containSubset(res, {
      is_invalid: true,
      is_invalid_reason: 'invalid channel state for event DidCreateChannel: got CS_OPEN but should be NULL',
    });

  });

  [
    // Intent comes first
    async () => {
      await pgm.insertChannelIntent(update(didCreateChannelEvent, { block_number: 1 }));
      await pgm.insertChannelEvent(update(didCreateChannelEvent, { block_number: 2 }));
    },

    // Channel event comes first
    async () => {
      await pgm.insertChannelEvent(update(didCreateChannelEvent, { block_number: 2 }));
      await pgm.insertChannelIntent(update(didCreateChannelEvent, { block_number: 1 }));
    },

    // Channel event is invalidated 1
    async () => {
      await pgm.insertChannelIntent(update(didCreateChannelEvent, { block_number: 1 }));
      await pgm.insertChannelEvent(update(didCreateChannelEvent, { block_number: 2, block_hash: mkhash('a') }));
      await pgm.insertChannelEvent(update(didCreateChannelEvent, { block_number: 2, block_hash: mkhash('b') }));
      await pgm.setRecentBlocks(1, 2, [mkhash('a')]);
      return mkhash('a');
    },

    // Channel event is invalidated 2
    async () => {
      await pgm.insertChannelIntent(update(didCreateChannelEvent, { block_number: 1 }));
      await pgm.insertChannelEvent(update(didCreateChannelEvent, { block_number: 2, block_hash: mkhash('a') }));
      await pgm.insertChannelEvent(update(didCreateChannelEvent, { block_number: 2, block_hash: mkhash('b') }));
      await pgm.setRecentBlocks(1, 2, [mkhash('b')]);
      return mkhash('b');
    },

  ].map((setup, idx) => {
    it(`Intent events: should be promoted to real events: ${idx}`, async () => {
      let canary1 = update(didCreateChannelEvent, { channel_id: mkchanid('canary1') });
      let canary2 = update(didCreateChannelEvent, { channel_id: mkchanid('canary2'), block_hash: null });
      await Promise.all([
        pgm.insertChannelEvent(canary1),
        pgm.insertChannelIntent(canary2),
      ]);

      let blockHash = await setup();

      let rows = await pgm.getChannelEvents(testChannel);
      assert.equal(rows.length, 1);
      assert.containSubset(rows[0], {
        block_number: 2,
        block_hash: blockHash || didCreateChannelEvent.block_hash,
      });

      await Promise.all([canary1, canary2].map(async canary => {
        delete canary.ts;
        rows = await pgm.getChannelEvents(canary);
        assert.equal(rows.length, 1);
        assert.containSubset(rows[0], canary);
      }));

    });

  });

  describe('mcy_pack_* functions', async () => {
    let packTests = [
      [0, 4, '00000000'],
      [1, 4, '00000001'],
      [0xaabb, 4, '0000aabb'],
      [0xaabbccdd, 4, 'aabbccdd'],
      [0xaa000000, 4, 'aa000000'],
      [0xaa00, 3, '00aa00'],
    ];
    packTests.forEach(test => {
      let [input, num, expected] = test;
      ['bigint', 'numeric'].forEach(type => {
        it(`pack_${type}_big_endian_bytes: ${input.toString(16)} => ${expected}`, async () => {
          let res = await pgm._queryOne('res', `
            select mcy_pack_${type}_big_endian_bytes(${num}, '${input}'::${type}) as res
          `);
          assert.equal(res, expected);
        });
      });
    });
  });


});
