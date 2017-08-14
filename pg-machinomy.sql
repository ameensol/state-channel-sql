--
-- Setup extensions
--
create extension if not exists hstore;
create extension if not exists pgcrypto;
create extension if not exists pguecc;

--
-- Data types
--

create domain mcy_eth_address as varchar(40)
check ( length(value) = 40 );

create domain mcy_sha3_hash as varchar(64)
check ( length(value) = 64 );

-- hopefully we'll never need to store more than 1e1000 wei...
create domain mcy_wei as numeric(1000);

-- use this to cast mcy_wei values to JSON
create domain mcy_wei_to_js as text;

create function mcy_wei_from_js(obj jsonb, field text)
returns mcy_wei
language plpgsql immutable as $pgsql$
declare
    val jsonb := (obj->field);
    val_type text := jsonb_typeof(val);
begin
    if val_type = 'string' then
        return (obj->>field)::mcy_wei;
    end if;

    raise exception 'wei values must be text (but % is %)', val, val_type;
end
$pgsql$;


create domain mcy_secp256k1_sig as varchar(130)
check ( length(value) = 130 );



--
-- State updates table
--

create table state_updates (
    id bigserial primary key,
    chain_id int not null,
    contract_id mcy_eth_address not null,
    channel_id mcy_sha3_hash not null,
    ts timestamp not null,

    amount mcy_wei not null,
    signature mcy_secp256k1_sig not null
);

create unique index state_updates_chain_contract_chan_seq_unique_idx
on state_updates (chain_id, contract_id, channel_id, amount);


/*
Converts a ``state_update`` to a ``jsonb`` object.

Public, but probably not especially useful (except internally).
*/
create function mcy_state_update_row_to_json(su state_updates)
returns jsonb
language plpgsql immutable called on null input as $pgsql$
begin
    if su is null then
        return 'null'::jsonb;
    end if;

    return json_build_object(
        'id', su.id,
        'chain_id', su.chain_id,
        'contract_id', su.contract_id,
        'channel_id', su.channel_id,
        'ts', su.ts,
        'amount', su.amount::mcy_wei_to_js,
        'signature', su.signature
    );
end
$pgsql$;


create table invalid_state_updates (
    id bigserial primary key,
    inserted_on timestamp not null,

    reason text not null,
    status jsonb not null,

    blob jsonb not null
);


--
-- Events
--

create type mcy_channel_event_type as enum (
    -- As defined in Machinomy's Broker.sol
    'DidCreateChannel',
    'DidDeposit',
    'DidStartSettle',
    'DidSettle'
);


/*
The Solidity events fired by by Machinomy's Broker.sol.

Insert events with ``mcy_insert_channel_event(...)``.

Used to build up a channel's state (see ``mcy_get_channel``).
*/
create table channel_events (
    id bigserial primary key,
    chain_id int not null,
    contract_id mcy_eth_address not null,
    channel_id mcy_sha3_hash not null,

    -- The block's timestamp, number, and hash.
    ts timestamp not null,
    block_number int not null,
    block_hash mcy_sha3_hash not null,

    -- Whether this block is valid and should be considered in the channel's
    -- state. Always ``true`` initially, but set to ``false`` by
    -- ``mcy_set_recent_blocks`` if this block is orphaned by a reorg.
    block_is_valid boolean not null,

    -- The event's sender
    sender mcy_eth_address not null,

    -- The event's type ('DidCreateChannel', etc) and event-specific fields
    -- as defined by the broker contract ('value', 'payment', etc).
    event_type mcy_channel_event_type not null,
    fields jsonb not null
);


create index channel_events_ordering_idx
on channel_events (chain_id, contract_id, channel_id, block_number, block_hash);


/*
"Intent" events have a similar structure to ``channel_events``, but are used
by the application server to register that it "intends" to see a particular
event at some point in the future.

For example, when a channel is settled, the application may insert an intent
event immediately so that the fact that a channel has been closed will be
reflected in subsequent calls to ``mcy_get_channel(...)``, even though the
"real" event hasn't yet been received from the blockchain.

Each time a "real" ``channel_event`` arrives the corresponding
``channel_intents``'s ``block_hash`` will be updated to
reflect the hash of the "real" event's block. This is done by the
``mcy_update_channel_intents_trigger()`` trigger.

Insert channel intents with ``mcy_insert_channel_intent(...)``.
*/
create table channel_intents (
    id bigserial primary key,
    chain_id int not null,
    contract_id mcy_eth_address not null,
    channel_id mcy_sha3_hash not null,

    -- The server's timestamp
    ts timestamp not null,

    -- The latest known block number at the time the intent was issued. This is
    -- used to enforce serialization between "intent" events and block-based
    -- events.
    block_number int not null,

    -- The block hash will be null until it has been correlated with a
    -- blockchain event, at which point it will be set to that block's hash.
    block_hash mcy_sha3_hash,

    -- Always true (to simplify compatibility with ``channel_events``).
    block_is_valid boolean null,

    -- The address we expect the transaction to be sent from.
    sender mcy_eth_address not null,

    -- Same as ``channel_events``.
    event_type mcy_channel_event_type not null,
    fields jsonb not null
);

/*
Converts a ``channel_events`` or ``channel_intents`` to a ``jsonb`` object.

Public, but probably not especially useful (except internally).
*/
create function mcy_channel_event_to_json(e anyelement)
returns jsonb
language plpgsql immutable called on null input as $pgsql$
begin
    if e is null then
        return 'null'::jsonb;
    end if;

    return row_to_json(e);
end
$pgsql$;


create index channel_intents_ordering_idx
on channel_intents (chain_id, contract_id, channel_id, block_number, block_hash, ts);

-- Internal. Finds the (possibly null) hash of a ``channel_events``'s block
-- which corresponds to a ``channel_intent``.
create function mcy_get_intent_block_hash(intent channel_intents)
returns mcy_sha3_hash
language sql as $$
    select block_hash
    from channel_events as ce
    where
        ce.chain_id = intent.chain_id and
        ce.contract_id = intent.contract_id and
        ce.channel_id = intent.channel_id and
        ce.block_number >= intent.block_number and
        ce.sender = intent.sender and
        ce.event_type = intent.event_type and
        ce.fields = intent.fields and
        ce.block_is_valid
    order by id desc limit 1;
$$;

-- Internal. Trigger which updates ``channel_intents.block_hash`` as
-- ``channel_events`` are inserted, updated, and removed.
create function mcy_update_channel_intents_trigger()
returns trigger
language plpgsql as $pgsql$
declare
    dummy boolean;
begin
    if TG_OP = 'INSERT' then
        update channel_intents as ci
        set block_hash = NEW.block_hash
        where
            ci.chain_id = NEW.chain_id and
            ci.contract_id = NEW.contract_id and
            ci.channel_id = NEW.channel_id and
            ci.block_number <= NEW.block_number and
            ci.sender = NEW.sender and
            ci.event_type = NEW.event_type and
            ci.fields = NEW.fields;
    else
        update channel_intents as ci
        set block_hash = mcy_get_intent_block_hash(ci)
        where
            ci.chain_id = OLD.chain_id and
            ci.contract_id = OLD.contract_id and
            ci.channel_id = OLD.channel_id and
            ci.block_number <= OLD.block_number and
            ci.sender = OLD.sender and
            ci.event_type = OLD.event_type and
            ci.fields = OLD.fields;
    end if;

    return null;
end
$pgsql$;

create trigger mcy_update_channel_intents_trigger
after insert or update or delete
on channel_events
for each row execute procedure mcy_update_channel_intents_trigger();


/*
Inserts a channel event (see ``channel_events``) and returns the channel's
status (see ``mcy_get_channel_status()``).

See documentation on ``PGMachinomy.insertChannelEvent(...)`` for the fields in
``event``.
*/
create function mcy_insert_channel_event(event jsonb)
returns jsonb
language plpgsql as $pgsql$
declare
    event_type text := (event->>'event_type');
    fields jsonb := (event->'fields');
    error jsonb;
begin
    insert into channel_events (
        chain_id, contract_id, channel_id,
        ts,
        block_hash, block_number, block_is_valid,
        sender, event_type, fields
    )
    values (
        (event->>'chain_id')::int,
        (event->>'contract_id')::mcy_eth_address,
        (event->>'channel_id')::mcy_sha3_hash,

        to_timestamp((event->>'ts')::float),

        (event->>'block_hash')::mcy_sha3_hash,
        (event->>'block_number')::int,
        true,

        (event->>'sender')::mcy_eth_address,
        (event->>'event_type')::mcy_channel_event_type,
        fields
    );

    return mcy_get_channel_status(event, true);
end
$pgsql$;


/*
Inserts a channel intent (see ``channel_intents``) and returns the channel's
status (see ``mcy_get_channel_status()``).

See documentation on ``PGMachinomy.insertChannelIntent(...)`` for the fields in
``event``.
*/
create function mcy_insert_channel_intent(intent jsonb)
returns jsonb
language plpgsql as $pgsql$
declare
    event_type text := (intent->>'event_type');
    fields jsonb := (intent->'fields');
    new_intent channel_intents;
begin
    insert into channel_intents (
        chain_id, contract_id, channel_id,
        ts,
        block_hash, block_number,
        sender, event_type, fields
    )
    values (
        (intent->>'chain_id')::int,
        (intent->>'contract_id')::mcy_eth_address,
        (intent->>'channel_id')::mcy_sha3_hash,

        now(),

        null,
        (intent->>'block_number')::int,

        (intent->>'sender')::mcy_eth_address,
        (intent->>'event_type')::mcy_channel_event_type,
        fields
    )
    returning * into new_intent;

    update channel_intents
    set block_hash = mcy_get_intent_block_hash(new_intent)
    where id = new_intent.id;

    return mcy_get_channel_status(intent, true);
end
$pgsql$;


/*
Handle reorgs by updating the database so that it's in sync with the current
blockchain state. ``chain_id`` is the block chain being updated,
``first_block_num`` is the block number of the first block in ``block_hashes``,
which is a list of block hashes.

See documentation on ``PGMachinomy.setRecentBlocks()`` for more.

Returns a JSON object::

    {
        "updated_event_count": int, // the number of events that were updated
        // the new states of all channels that were changed (see
        // ``mcy_get_channel_status``)
        "updated_channels": [...],  
    }
*/

create function mcy_set_recent_blocks(chain_id_ int, first_block_num int, block_hashes text[])
returns jsonb
language plpgsql as $pgsql$
declare
    ue record;
    updated_event_count int := 0;
    contract_key text;
    updated_contracts_hstore hstore := ''::hstore;
    updated_contracts_list jsonb[];
begin
    for ue in
        with events_with_is_valid as (
            select
                events.id,
                events.block_hash = coalesce(
                    block_hashes[block_number - first_block_num + 1],
                    '<invalid>'
                ) as new_is_valid
            from channel_events as events
            where
                chain_id = chain_id_ and
                block_hash is not null and
                block_number >= first_block_num
        )
        update channel_events as e
        set block_is_valid = new_is_valid
        from events_with_is_valid as u
        where
            e.id = u.id and
            e.block_is_valid <> u.new_is_valid
        returning e.*, u.new_is_valid
    loop
        updated_event_count := updated_event_count + 1;
        contract_key := ue.chain_id || ' ' || ue.contract_id || ' ' || ue.channel_id;
        if (updated_contracts_hstore->contract_key) is not null then
            continue;
        end if;
        updated_contracts_hstore := updated_contracts_hstore || hstore(contract_key, 'true');
        updated_contracts_list := array_append(updated_contracts_list, json_build_object(
            'chain_id', ue.chain_id,
            'contract_id', ue.contract_id,
            'channel_id', ue.channel_id
        )::jsonb);
    end loop;

    return json_build_object(
        'updated_event_count', updated_event_count,
        'updated_channels', array((
            select mcy_get_channel_status(chan, true)
            from unnest(updated_contracts_list) as x(chan)
        ))
    );
end
$pgsql$;


--
-- Channels
--

create type mcy_channel_state as enum (
    'CS_OPEN',     -- Open for business
    'CS_SETTLING', -- Sender has requested settlement
    'CS_SETTLED'   -- Channel has been settled
);

/*
The state of a Machinomy payment channel. Generated by ``mcy_get_channel()``.

A type instead of a table because channel states are purely aggregate, and for
the moment they are rebuilt from channel events on demand (if this ever proves
to be too slow, it will be straight forward to introduce a caching layer).
*/
create type mcy_channel as (
    chain_id int,
    contract_id mcy_eth_address,
    channel_id mcy_sha3_hash,

    last_state_id bigint,
    last_event_id bigint,

    sender mcy_eth_address,
    receiver mcy_eth_address,
    value mcy_wei,
    settlement_period int,
    until timestamp,

    payment mcy_wei,
    odd_value mcy_wei,

    state mcy_channel_state,
    state_is_intent boolean,

    opened_on timestamp,
    settlement_started_on timestamp,
    settlement_finalized_on timestamp
);


/*
Converts an ``mcy_channel`` to a ``jsonb`` object.

Public, but probably not especially useful (except internally).
*/
create function mcy_channel_row_to_json(c mcy_channel)
returns jsonb
language plpgsql immutable called on null input as $pgsql$
begin
    if c is null then
        return 'null'::jsonb;
    end if;

    return json_build_object(
        'chain_id', c.chain_id,
        'contract_id', c.contract_id,
        'channel_id', c.channel_id,

        'last_state_id', c.last_state_id,
        'last_event_id', c.last_event_id,

        'sender', c.sender,
        'receiver', c.receiver,
        'value', c.value::mcy_wei_to_js,
        'settlement_period', c.settlement_period,
        'until', c.until,

        'payment', c.payment::mcy_wei_to_js,
        'odd_value', c.odd_value::mcy_wei_to_js,

        'state', c.state,
        'state_is_intent', c.state_is_intent,

        'opened_on', c.opened_on,
        'settlement_started_on', c.settlement_started_on,
        'settlement_finalized_on', c.settlement_finalized_on
    );
end
$pgsql$;

/*
Returns all of the events that pertain to a channel in their canonical order
(ie, oldest first), excluding events from blocks known to be orphaned. If
``include_intent`` is set, "intent" invents will be included (not just "real"
events from the block chain).
*/
create function mcy_get_channel_events(chan jsonb, include_intent boolean)
returns setof channel_events
language sql as $$
    (
        select *
        from channel_events
        where
            chain_id = (chan->>'chain_id')::int and
            contract_id = (chan->>'contract_id')::mcy_eth_address and
            channel_id = (chan->>'channel_id')::mcy_sha3_hash and
            block_is_valid
    )
    union all
    (
        select *
        from channel_intents
        where
            include_intent and
            chain_id = (chan->>'chain_id')::int and
            contract_id = (chan->>'contract_id')::mcy_eth_address and
            channel_id = (chan->>'channel_id')::mcy_sha3_hash and
            block_hash is null
    )
    order by block_number, block_hash, ts
$$;


create type mcy_get_channel_result as (
    channel mcy_channel,
    latest_intent_event channel_events,
    latest_chain_event channel_events,
    latest_event channel_events,

    is_invalid bool,
    is_invalid_reason text
);

/*
Gets the state of a channel (see ``mcy_channel``).

The ``chan`` should be a ``jsonb`` object with ``chain_id``, ``contract_id``,
and ``channel_id`` fields.

Safe to use externally, but ``mcy_get_channel_status`` is more likely to be
useful to external callers.

At the moment channels are re-built from their event on demand, but in the
future a cache table may be introduced, and that table would be used by this
function.
*/
create function mcy_get_channel(chan jsonb, include_intent boolean)
returns mcy_get_channel_result
language plpgsql as $pgsql$
declare
    res mcy_get_channel_result;
    event channel_events;
    apply_res record;
begin
    for event in (select * from mcy_get_channel_events(chan, include_intent)) loop
        res.latest_event = event;
        if event.block_hash is null then
            res.latest_intent_event := event;
        else
            res.latest_chain_event := event;
        end if;

        apply_res := mcy_channel_apply_event(res.channel, event);

        res.is_invalid := apply_res.is_invalid;
        res.is_invalid_reason := apply_res.is_invalid_reason;

        if res.is_invalid then
            exit;
        else
            res.channel = apply_res.new_channel;
        end if;

    end loop;

    return res;
end
$pgsql$;


/*
Gets a channel's status, which includes the ``mcy_channel`` and information
about the current balance, last event, etc.

Likely the most useful function to external callers.

See code or ``PGMachinomy.getChannelStatus()`` for complete documentation.
*/
create function mcy_get_channel_status(chan jsonb, include_intent boolean)
returns jsonb
language plpgsql as $pgsql$
declare
    cr mcy_get_channel_result;
    latest_event channel_events;
    latest_intent_event channel_events;
    latest_state state_updates;

    channel mcy_channel;
begin
    cr := mcy_get_channel(chan, include_intent);
    channel := cr.channel;
    latest_state := mcy_get_latest_state_update(chan);

    return json_build_object(
        'channel', mcy_channel_row_to_json(channel),
        'latest_state', mcy_state_update_row_to_json(latest_state),

        'current_payment', latest_state.amount::mcy_wei_to_js,
        'current_remaining_balance', (channel.value - latest_state.amount)::mcy_wei_to_js,

        'latest_event', mcy_channel_event_to_json(cr.latest_event),
        'latest_intent_event', mcy_channel_event_to_json(cr.latest_intent_event),
        'latest_chain_event', mcy_channel_event_to_json(cr.latest_chain_event),

        'is_invalid', cr.is_invalid,
        'is_invalid_reason', cr.is_invalid_reason
    );

end
$pgsql$;


/*
Internal.

Update a (possibly null) ``channel mcy_channel`` in response to an
``event channel_events``.

Consider this the "reduction" step in a map/reduce.

Will return with ``is_invalid = true`` and ``is_invalid_reason`` set to a
developer-friendly explanation of the error if the event would yield an
obviously invalid channel (ex, if two ``DidCreateChannel`` events are received
for the same channel. When ``is_invalid = true`` the original (ie, non-updated)
channel will be returned.

For example:

    =# chan := mcy_channel_apply_event(null, row(
    .#     type='DidCreateChannel',
    .#     fields='{"sender": "abc...", "receiver": "123...", "value": 42, ...},
    .#     ...
    .# ));
    =# print chan;
    row(
        new_channel=mcy_channel(state='CS_OPEN', sender='abc...', receiver='123...', value=42, ...)),
        is_invalid=false,
        is_invalid_reason=null,
    );
    =# print mcy_channel_apply_event(chan, row(type='DidCreateChannel', ...));
    row(
        new_channel=mcy_channel(state='CS_OPEN', ...),
        is_invalid=true,
        is_invalid_reason='invalid channel state for event DidCreateChannel: got CS_OPEN but should be NULL.',
    )
    =# print mcy_channel_apply_event(chan, row(type='DidDeposit', fields='{"value": 25}', ...));
    row(
        new_channel=mcy_channel(state='CS_OPEN', value=67, ...)),
        is_invalid=false,
        is_invalid_reason=null,
    );
*/

create function mcy_channel_apply_event(
    in channel mcy_channel, in event channel_events,
    out new_channel mcy_channel, out is_invalid boolean, out is_invalid_reason text
)
returns record
language plpgsql immutable as $pgsql$
declare
    event_type text := event.event_type;
    fields jsonb := event.fields;
begin
    if channel is null then
        channel.chain_id = event.chain_id;
        channel.contract_id = event.contract_id;
        channel.channel_id = event.channel_id;
        channel.last_event_id = event.id;
    end if;

    if event_type = 'DidCreateChannel' then
        is_invalid_reason := mcy_assert_channel_state(event_type, channel, NULL);
        channel.state = 'CS_OPEN';
        channel.opened_on = event.ts;
        channel.sender = mcy_not_null(fields, 'sender')::mcy_eth_address;
        channel.receiver = mcy_not_null(fields, 'receiver')::mcy_eth_address;
        channel.settlement_period = mcy_not_null(fields, 'settlement_period')::int;
        channel.until = to_timestamp(mcy_not_null(fields, 'until')::float);
        channel.value = mcy_wei_from_js(fields, 'value');
    elsif event_type = 'DidDeposit' then
        is_invalid_reason := mcy_assert_channel_state(event_type, channel, 'CS_OPEN');
        channel.value = channel.value + mcy_wei_from_js(fields, 'value');
    elsif event_type = 'DidStartSettle' then
        is_invalid_reason := mcy_assert_channel_state(event_type, channel, 'CS_OPEN');
        channel.state = 'CS_SETTLING';
        channel.settlement_started_on = event.ts;
        channel.until = event.ts + (channel.settlement_period * (interval '1 second'));
        channel.payment = mcy_not_null(fields, 'payment')::mcy_wei;
    elsif event_type = 'DidSettle' then
        is_invalid_reason := mcy_assert_channel_state(event_type, channel, 'CS_OPEN', 'CS_SETTLING');
        channel.state = 'CS_SETTLED';
        channel.settlement_finalized_on = event.ts;
        channel.payment = mcy_not_null(fields, 'payment')::mcy_wei;
        channel.odd_value = mcy_not_null(fields, 'odd_value')::mcy_wei;
    else
        is_invalid_reason := 'invalid event type: ' || event_type;
    end if;

    channel.state_is_intent := coalesce(channel.state_is_intent, false) or (event.block_hash is null);
    new_channel = channel;
    is_invalid := is_invalid_reason is not null;
end
$pgsql$;

-- Internal. Used by mcy_channel_apply_event to ensure a channel's state is
-- sensible before being updated.
create function mcy_assert_channel_state(event_type text, channel mcy_channel, a text, b text default null)
returns text
language plpgsql immutable as $pgsql$
declare
    chan_state text := coalesce(channel.state::text, 'NULL');
begin
    a = coalesce(a, 'NULL');
    if chan_state = a or chan_state = b then
        return null;
    end if;

    return format(
        'invalid channel state for event %s: got %s but should be %s',
        event_type,
        chan_state,
        case when b is null then a else a || ' or ' || b end
    );
end
$pgsql$;


--
-- Functions
--

/*
Checks the status - signature validity, whether it's a duplicate, and whether
it's the latest - of a state update without inserting it into the database.

See ``PGMachinomy.getStateUpdateStatus()``.
*/
create function mcy_get_state_update_status(state_update jsonb)
returns jsonb
language plpgsql as $pgsql$
declare
    last_status state_updates;
    amount mcy_wei := mcy_wei_from_js(state_update, 'amount');
    is_latest boolean;
begin
    -- signature_valid: true | false
    -- is_latest: true | false | null (if sig isn't valid)
    -- dupe_status: 'distinct' | 'duplicate' | 'conflict' | null
    -- added_amount: eth | null (the amount of eth this update adds to the
    --   channel; null if this update isn't the latest)

    last_status := mcy_get_latest_state_update(state_update);

    is_latest := coalesce(amount >= last_status.amount, true);

    return json_build_object(
        'signature_valid', mcy_state_update_is_signature_valid(state_update),
        'is_latest', is_latest,
        'added_amount', (
            case when is_latest
            then amount - coalesce(last_status.amount, 0)
            else null
            end
        )::mcy_wei_to_js,
        'dupe_status', (
            case when coalesce(last_status.amount = amount, false)
            then 'dupe'
            else 'distinct'
            end
        )
    );
end
$pgsql$;

-- Internal. Checks the signature on a state update.
create function mcy_state_update_is_signature_valid(state_update jsonb)
returns boolean
language plpgsql immutable as $pgsql$
declare
    is_valid boolean;
    amount mcy_wei := mcy_wei_from_js(state_update, 'amount');
    to_hash bytea;
begin
    to_hash := (
        '\x' ||
        mcy_pack_bigint_big_endian_bytes(4, (state_update->>'chain_id')::int) ||
        lpad((state_update->>'contract_id'), 40, '0') ||
        lpad((state_update->>'channel_id'), 64, '0') ||

        mcy_pack_numeric_big_endian_bytes(32, amount * 1e18)
    )::bytea;

    return coalesce(ecdsa_verify(
        state_update->>'sender',
        to_hash,
        (state_update->>'r') || (state_update->>'s'),
        'sha256'::text,
        'secp256k1'::text
    ), false);
end
$pgsql$;


/*
Inserts a state update into the database, returning whether or not the update
was valid (and inserting it into the ``invalid_state_updates`` table if not)
and the new balance of the payment channel.

See ``PGMachinomy.insertStateUpdate()`` for full details.
*/
create function mcy_insert_state_update(state_update jsonb, include_intent bool default true)
returns jsonb
language plpgsql as $pgsql$
declare
    status jsonb;

    new_row state_updates;
    latest_state state_updates;
    channel mcy_channel;

    amount mcy_wei;
begin
    -- TODO: make sure we're doing the appropriate locking here so that
    -- state updates are serialized.
    status := mcy_get_state_update_status(state_update);

    if not (status->>'signature_valid')::boolean then
        return mcy_insert_invalid_state_update(
            'signature_invalid', status, state_update
        );
    end if;

    if (status->>'dupe_status') = 'conflict' then
        return mcy_insert_invalid_state_update(
            'conflict', status, state_update
        );
    end if;

    amount := mcy_wei_from_js(state_update, 'amount');
    if amount < 0 then
        return mcy_insert_invalid_state_update(
            'negative_amount', status, state_update
        );
    end if;

    if (status->>'dupe_status') = 'distinct' then
        insert into state_updates (
            chain_id, contract_id, channel_id, ts, amount, signature
        )
        select
            (state_update->>'chain_id')::int as chain_id,
            (state_update->>'contract_id')::mcy_eth_address as contract_id,
            (state_update->>'channel_id')::mcy_sha3_hash as channel_id,
            to_timestamp((state_update->>'ts')::float) as ts,
            amount,
            (state_update->>'signature')::mcy_secp256k1_sig as signature
        returning * into new_row;
    end if;

    latest_state := (
        case when (status->>'is_latest')::boolean and new_row is not null
        then new_row
        else mcy_get_latest_state_update(state_update)
        end
    );

    channel := (mcy_get_channel(state_update, include_intent)).channel;

    return json_build_object(
        'id', new_row.id,
        'created', (new_row.id is not null),
        'status', status,
        'is_latest', status->'is_latest',
        'latest_state', (mcy_state_update_row_to_json(latest_state)),
        'added_amount', status->'added_amount',
        'channel_payment', (latest_state.amount)::mcy_wei_to_js,
        'channel_remaining_balance', (channel.value - latest_state.amount)::mcy_wei_to_js
    );
end
$pgsql$;


-- Internal. Inserts an invalid state update into the ``invalid_state_updates``
-- table and returns an error.
create function mcy_insert_invalid_state_update(reason text, status jsonb, blob jsonb)
returns jsonb
language plpgsql as $pgsql$
begin
    insert into invalid_state_updates (inserted_on, reason, status, blob)
    values (now(), reason, status, blob);

    return json_build_object(
        'error', true,
        'status', status,
        'reason', reason
    );
end
$pgsql$;


/*
Gets the latest state update for a particular channel.
*/
create function mcy_get_latest_state_update(chan jsonb)
returns state_updates
language plpgsql stable called on null input as $pgsql$
declare
    chain_id_ int := mcy_not_null(chan, 'chain_id');
    contract_id_ mcy_eth_address := mcy_not_null(chan, 'contract_id');
    channel_id_ mcy_sha3_hash := mcy_not_null(chan, 'channel_id');
    latest_state state_updates;
begin
    select sup.*
    from state_updates as sup
    where
        sup.chain_id = chain_id_ and
        sup.contract_id = contract_id_ and
        sup.channel_id = channel_id_
    order by amount desc
    limit 1
    into latest_state;

    return latest_state;
end
$pgsql$;


-- Internal.
-- Packs ``val`` into a hex-encoded string, left padded to ``num_bytes`` bytes::
--
--     =# select mcy_pack_numeric_big_endian_bytes(4, 0xAABB);
--     '0000AABB'
--
-- Will raise an error if ``val`` is non-integral, or if the result would
-- be too large to fit into ``num_bytes`` bytes.
create function mcy_pack_numeric_big_endian_bytes(num_bytes integer, val numeric)
returns text
language plpgsql immutable as $pgsql$
declare
    result text := '';
    orig_val numeric := val;
    byte_val integer;
    byte_hex varchar(2);
begin
    if val != trunc(val) then
        raise exception 'value must not have decimal places: %', orig_val;
    end if;

    while val > 0 loop
        byte_val := (val % 256)::int;
        byte_hex := to_hex(byte_val);
        val := trunc(val / 256);
        if byte_val < 16 then
            byte_hex := '0' || byte_hex;
        end if;
        result := byte_hex || result;
    end loop;

    if (length(result) / 2) > num_bytes then
        raise exception 'value too large for % bytes: %', num_bytes, orig_val;
    end if;

    result := repeat('00', num_bytes - (length(result) / 2)) || result;
    return result;
end
$pgsql$;

-- Internal.
-- See documentation on ``mcy_pack_numeric_big_endian_bytes``.
-- Duplicated function for efficiency (``bigint`` operations are faster than
-- ``numeric`` operations).
create function mcy_pack_bigint_big_endian_bytes(num_bytes integer, val bigint)
returns text
language plpgsql immutable as $pgsql$
declare
    result text := '';
    orig_val bigint := val;
    byte_val bigint;
    byte_hex varchar(2);
begin
    if val != trunc(val) then
        raise exception 'value must not have decimal places: %', orig_val;
    end if;

    while val > 0 loop
        byte_val := (val % 256)::int;
        byte_hex := to_hex(byte_val);
        val := trunc(val / 256);
        if byte_val < 16 then
            byte_hex := '0' || byte_hex;
        end if;
        result := byte_hex || result;
    end loop;

    if (length(result) / 2) > num_bytes then
        raise exception 'value too large for % bytes: %', num_bytes, orig_val;
    end if;

    result := repeat('00', num_bytes - (length(result) / 2)) || result;
    return result;
end
$pgsql$;

-- Internal. Extracts ``field`` from ``jsonb obj`` and throws an exception
-- if the result (or any of the input) is null.
create function mcy_not_null(obj jsonb, field text)
returns text
language plpgsql immutable called on null input as $pgsql$
declare
    res text := (obj->>field);
begin
    if res is null then
        raise exception '% must not be null', field using errcode = 'null_value_not_allowed';
    end if;
    return res;
end
$pgsql$;
