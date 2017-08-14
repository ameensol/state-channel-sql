pg-machinomy: Machinomy payment channels implemented in Postgres
================================================================

``pg-machinomy`` is an implementation of `Machinomy`__ payment channels
implemented entirely in PostgreSQL, with an accompanying node interface.

__ https://github.com/machinomy/machinomy


Installation
------------

node package
............

**TODO**: make an ``npm install``-able package.


PostgreSQL
..........

To setup the Postgres database:

1. Create a database::

    $ createdb your-pg-machinomy-db

2. Load the SQL, either directly::

    $ psql your-pg-machinomy-db < pg-machinomy.sql

   Or through the node package::

    let { PGMachinomy } = require('pg-machinomy');

    let pgm = new PGMachinomy({
      database: 'your-pg-machinomy-db',
      [user: 'some-user',]
      [password: '...',]
      [host: 'some-host',],
    })

    await pgm.setupDatabase();
    await pgm.selftest();

Usage
-----

After the database has been setup, the ``pg-machinomy`` node package can be used::

    let { PGMachinomy } = require('pg-machinomy');

    let pgm = new PGMachinomy(<connection info>);

Where the ``<connection info>`` is either a string::

    let pgm = new PGMachinomy("postgres://user:password@host/database");

Or an object::

    let pgm = new PGMachinomy({
      database: 'your-pg-machinomy-db',
      [user: 'some-user',]
      [password: '...',]
      [host: 'some-host',],
    });



The three main methods are ``PGMachinomy.insertStateUpdate(...)``,
``PGMachinomy.insertChannelEvent(...)``, and
``PGMachinomy.getChannelStatus(...)``.

``PGMachinomy.insertStateUpdate(...)`` inserts a state update (ie, payment)
and returns the new channel state::

    pgm.insertStateUpdate({
      // 1 for mainnet, 3 for ropsten testnet
      chain_id: 1,
      // the ID of the contract this state udpate is intended for
      contract_id: "abc123...",
      // the channel ID (as per the DidCreateChannel event)
      channel_id: "abc123...",

      // The amount, in wei (note: must be a string; numbers will be rejected).
      amount: "123",

      // The state update's signature
      signature: "abc123..."
    })

And returns::

  {
    // If the state was inserted, the ID of the new row in the
    // state_updates table. Nothing will be inserted if the new state is
    // an exact duplicate of an existing state
    id: null | integer
    // If the new state was inserted (see above)
    created: true | false
    status: StateUpdateStatus
    is_latest: true | false
    latest_state: StateUpdate of the latest state in the channel
    channel_payment: wei_amount (identical to latest_state.amount)
    // If a channel exists and has a value, `channel.value - latest_state.amount`.
    channel_remaining_balance: null | wei_amount
    // The amount of wei this update adds to the channel. See
    // StateUpdateStatus.added_amount.
    added_amount: wei_amount | null
  }

Where::

  StateUpdate = {
    chain_id: int,
    contract_id: eth_address,
    channel_id: sha3_hash,
    ts: Date,

    amount: wei_amount,
    signature: secp256k1_sig,
 }

And on error returns::

  {
    error: true,
    status: StateUpdateStatus
    reason: 'signature_invalid' | 'conflict' | 'invalid_state: <developer friendly reason>'
  }



``PGMachinomy.insertChannelEvent(...)`` inserts an event, as dispatched by the
Machinomy smart contract.

**TODO**: Document how to use web3 to send events.

``PGMachinomy.getChannelStatus(...)`` gets the current status of a channel::

    pgm.getChannelStatus({
      chain_id: 1,
      contract_id: "abc123...",
      channel_id: "abc123...",
    })

And returns::

  ChannelStatus = {
    channel: Channel

    latest_event: null | ChannelEvent | IntentEvent,
    latest_intent_event: null | IntentEvent,
    latest_chain_event: ChannelEvent

    latest_state: null | StateUpdate,

    // `current_payment` and `current_remaining_balance` are
    // shortcuts for `current_payment = latest_state.amount` and
    // `channel.value - current_payment`.
    current_payment: null | wei_amount,
    current_remaining_balance: null | wei_amount,

    is_invalid: boolean,
    is_invalid_reason: null | developer-friendly reason
  }

Where::

  Channel = {
    chain_id: int,
    contract_id: eth_address,
    channel_id: sha3_hash,

    sender: null | eth_address,
    receiver: null | eth_address,
    value: null | wei_amount,
    settlement_period: null | int,
    until: null | timestamp,

    payment: null | wei_amount,
    odd_value: null | wei_amount,

    // If any of the events related to this channel are intent events,
    // state_is_intent will be true, meaning the state represented here isn't
    // yet confirmed on the blockchain.
    state_is_intent: null | boolean,
    state: null | 'CS_OPEN' | 'CS_SETTLING' | 'CS_SETTLED',

    opened_on: null | timestamp,
    settlement_started_on: null | timestamp,
    settlement_finalized_on: null | timestamp
  }


Testing
=======

To test ``pg-machinomy``:

1. Make sure Postgres >= 9.4 is installed and running locally with ident auth::

    $ psql <<< "select current_setting('server_version_num')"
     current_setting 
    -----------------
     90403
    (1 row)


2. Install required packages::

    $ npm install


3. Run tests::

    $ npm test


Benchmarking
============

Benchmark with::

    $ ./benchmark

And be sure to see the note at the top of the ``benchmark`` script to
understand the meaning of the numbers.
