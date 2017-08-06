--
-- Setup extensions
--
create extension if not exists hstore;

--
-- Data types
--

create domain mcy_eth_address as varchar(40)
check ( length(value) = 40 );

create domain mcy_sha3_hash as varchar(64)
check ( length(value) = 64 );

-- hopefully we'll never need to store more than 1e(1000-18) either...
create domain mcy_eth as numeric(1000, 18);

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

    sequence_num int not null,
    amount mcy_eth not null,
    signature mcy_secp256k1_sig not null
);

create unique index state_updates_chain_contract_chan_seq_unique_idx
on state_updates (chain_id, contract_id, channel_id, sequence_num);


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


create table channel_events (
    id bigserial primary key,
    chain_id int not null,
    contract_id mcy_eth_address not null,
    channel_id mcy_sha3_hash not null,

    -- For blockchain events, the "ts" will be the block's timestamp. For
    -- "intent" events, the "ts" will be the server timestamp at the time the
    -- event was received.
    ts timestamp not null,

    -- For "intent" events the block_number is the latest known block number at
    -- the time the intent was issued. This is used to enforce serialization
    -- between "intent" events and block-based events.
    block_number int not null,

    -- For "intent" events, the block hash will be null until it has been
    -- correlated with a blockchain event, at which point it will be set to the
    -- block's hash.
    block_hash mcy_sha3_hash not null,

    -- Will always be true for intent events.
    block_is_valid boolean not null,

    -- For "intent" events, the "sender" is the address we expect the
    -- transaction to be sent from.
    sender mcy_eth_address not null,

    event_type mcy_channel_event_type not null,

    fields jsonb not null
);


create index channel_events_ordering_idx
on channel_events (chain_id, contract_id, channel_id, block_number, block_hash, ts);


create table channel_intents (
    id bigserial primary key,
    chain_id int not null,
    contract_id mcy_eth_address not null,
    channel_id mcy_sha3_hash not null,

    -- For blockchain events, the "ts" will be the block's timestamp. For
    -- "intent" events, the "ts" will be the server timestamp at the time the
    -- event was received.
    ts timestamp not null,

    -- For "intent" events the block_number is the latest known block number at
    -- the time the intent was issued. This is used to enforce serialization
    -- between "intent" events and block-based events.
    block_number int not null,

    -- For "intent" events, the block hash will be null until it has been
    -- correlated with a blockchain event, at which point it will be set to the
    -- block's hash.
    block_hash mcy_sha3_hash,

    -- Will always be true for intent events.
    block_is_valid boolean null,

    -- For "intent" events, the "sender" is the address we expect the
    -- transaction to be sent from.
    sender mcy_eth_address not null,

    event_type mcy_channel_event_type not null,

    fields jsonb not null
);


create index channel_intents_ordering_idx
on channel_events (chain_id, contract_id, channel_id, block_number, block_hash, ts);

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

    return mcy_get_channel(event, true);
end
$pgsql$;


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

    return mcy_get_channel(intent, true);
end
$pgsql$;



-- Updates the database so that it's in sync with the current blockchain state.
-- ``chain_id`` is the block chain being updated, ``first_block_num`` is the
-- block number of the first block in ``block_hashes``, which is a list of
-- block hashes.
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
            select mcy_get_channel(chan, false)
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

-- The 'channels' table is purely aggregate and should not be written to
-- directly. It will be updated by `mcy_refresh_channel(chan)`

create table channels_cache (
    id bigserial primary key,

    chain_id int not null,
    contract_id mcy_eth_address not null,
    channel_id mcy_sha3_hash not null,

    last_state_id bigint references state_updates(id),
    last_event_id bigint references channel_events(id),

    sender mcy_eth_address,
    receiver mcy_eth_address,
    value mcy_eth,
    settlement_period int,
    until timestamp,

    payment mcy_eth,
    odd_value mcy_eth,

    state mcy_channel_state,
    state_is_intent boolean,

    opened_on timestamp,
    settlement_started_on timestamp,
    settlement_finalized_on timestamp
);

-- Returns all of the events that pertain to a channel in their canonical order
-- (ie, oldest first), excluding events from blocks known to be orphaned. If
-- ``include_intent`` is set, "intent" invents will be included (not just
-- events from the block chain).
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


create function mcy_get_channel(chan jsonb, include_intent boolean)
returns jsonb
language plpgsql as $pgsql$
declare
    channel channels_cache;

    apply_res record;
    event channel_events;
    latest_intent_event channel_events;
    latest_chain_event channel_events;

    is_invalid boolean := null;
    is_invalid_reason text := null;

    latest_state state_updates;
begin
    channel := null;
    event := null;
    latest_intent_event := null;
    latest_chain_event := null;

    for event in (select * from mcy_get_channel_events(chan, include_intent)) loop
        if event.block_hash is null then
            latest_intent_event := event;
        else
            latest_chain_event := event;
        end if;

        apply_res := mcy_channel_apply_event(channel, event);

        is_invalid := apply_res.is_invalid;
        is_invalid_reason := apply_res.is_invalid_reason;

        if is_invalid then
            exit;
        else
            channel = apply_res.new_channel;
        end if;

    end loop;

    latest_state := mcy_get_latest_state_update(chan);

    return json_build_object(
        'channel', mcy_null_row_to_json(channel),
        'latest_state', mcy_state_update_row_to_json(latest_state),

        'current_payment', latest_state.amount,
        'current_remaining_balance', channel.value - latest_state.amount,

        'latest_event', mcy_null_row_to_json(event),
        'latest_intent_event', mcy_null_row_to_json(latest_intent_event),
        'latest_chain_event', mcy_null_row_to_json(latest_chain_event),

        'is_invalid', is_invalid,
        'is_invalid_reason', is_invalid_reason
    );

end
$pgsql$;


create function mcy_channel_apply_event(
    in channel channels_cache, in event channel_events,
    out new_channel channels_cache, out is_invalid boolean, out is_invalid_reason text
)
returns record
language plpgsql as $pgsql$
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

    -- TODO: check for invalid updates

    if event_type = 'DidCreateChannel' then
        is_invalid_reason := mcy_assert_channel_state(event_type, channel, NULL);
        channel.state = 'CS_OPEN';
        channel.opened_on = event.ts;
        channel.sender = (fields->>'sender')::mcy_eth_address;
        channel.receiver = (fields->>'receiver')::mcy_eth_address;
        channel.settlement_period = (fields->>'settlement_period')::int;
        channel.until = to_timestamp((fields->>'until')::float);
        channel.value = 0;
    elsif event_type = 'DidDeposit' then
        is_invalid_reason := mcy_assert_channel_state(event_type, channel, 'CS_OPEN');
        channel.value = coalesce(channel.value, 0) + (fields->>'value')::mcy_eth;
    elsif event_type = 'DidStartSettle' then
        is_invalid_reason := mcy_assert_channel_state(event_type, channel, 'CS_OPEN');
        channel.state = 'CS_SETTLING';
        channel.settlement_started_on = event.ts;
        channel.until = event.ts + (channel.settlement_period * (interval '1 second'));
        channel.payment = (fields->>'payment')::mcy_eth;
    elsif event_type = 'DidSettle' then
        is_invalid_reason := mcy_assert_channel_state(event_type, channel, 'CS_OPEN', 'CS_SETTLING');
        channel.state = 'CS_SETTLED';
        channel.settlement_finalized_on = event.ts;
        channel.payment = (fields->>'payment')::mcy_eth;
        channel.odd_value = (fields->>'odd_value')::mcy_eth;
    else
        is_invalid_reason := 'invalid event type: ' || event_type;
    end if;

    channel.state_is_intent := coalesce(channel.state_is_intent, false) or (event.block_hash is null);
    new_channel = channel;
    is_invalid := is_invalid_reason is not null;
end
$pgsql$;


create function mcy_assert_channel_state(event_type text, channel channels_cache, a text, b text default null)
returns text
language plpgsql as $pgsql$
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

create function mcy_state_update_status(state_update jsonb)
returns jsonb
language plpgsql as $pgsql$
declare
    is_valid boolean := mcy_state_update_is_signature_valid(state_update);
begin
    -- TODO: actaully check the state
    -- signature_valid: true | false
    -- is_latest: true | false | null (if sig isn't valid)
    -- dupe_status: 'distinct' | 'duplicate' | 'conflict' | null
    return ('{
        "signature_valid": ' || is_valid || ',
        "is_latest": true,
        "dupe_status": "distinct"
    }')::jsonb;
end
$pgsql$;

create function mcy_state_update_is_signature_valid(state_update jsonb)
returns boolean
language sql as $$
    -- TODO: Actaully figure out how to check this signature
    select true::boolean as signature_valid
$$;

create function mcy_state_update_row_to_json(state_update state_updates)
returns jsonb
language sql as $$
    select row_to_json(state_update)::jsonb;
$$;

create function mcy_insert_state_update(state_update jsonb)
returns jsonb
language plpgsql as $pgsql$
declare
    status jsonb;

    new_row state_updates;
    latest_state state_updates;
    remaining_balance mcy_eth;
begin
    -- TODO: make sure we're doing the appropriate locking here so that
    -- state updates are serialized.
    status := mcy_state_update_status(state_update);

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

    if (status->>'dupe_status') = 'distinct' then
        declare
            err_msg text;
        begin
            insert into state_updates (
                chain_id, contract_id, channel_id, ts, sequence_num, amount, signature
            )
            select
                (state_update->>'chain_id')::int as chain_id,
                (state_update->>'contract_id')::mcy_eth_address as contract_id,
                (state_update->>'channel_id')::mcy_sha3_hash as channel_id,
                to_timestamp((state_update->>'ts')::float) as ts,
                (state_update->>'sequence_num')::int as sequence_num,
                (state_update->>'amount')::mcy_eth as amount,
                (state_update->>'signature')::mcy_secp256k1_sig as signature
            returning * into new_row;
        exception when others then
            get stacked diagnostics err_msg = MESSAGE_TEXT;
            return mcy_insert_invalid_state_update(
                'invalid_state: ' || err_msg, status, state_update
            );
        end;
    end if;

    latest_state := (
        case when (status->>'is_latest')::boolean and new_row is not null
        then new_row
        else mcy_get_latest_state_update(state_update)
        end
    );

    return json_build_object(
        'id', new_row.id,
        'created', (new_row.id is not null),
        'status', status,
        'is_latest', status->'is_latest',
        'latest_state', (mcy_state_update_row_to_json(latest_state)),
        'channel_payment', latest_state.amount,
        'channel_remaining_balance', remaining_balance
    );
end
$pgsql$;


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


create function mcy_get_latest_state_update(chan jsonb)
returns state_updates
called on null input
language plpgsql as $pgsql$
declare
    chain_id_ int := (chan->>'chain_id')::int;
    contract_id_ mcy_eth_address := (chan->>'contract_id')::mcy_eth_address;
    channel_id_ mcy_sha3_hash := (chan->>'channel_id')::mcy_sha3_hash;
    latest_state state_updates;
begin
    if chain_id_ is null then
        raise exception 'chain_id must not be null' using errcode = 'null_value_not_allowed';
    end if;

    if contract_id_ is null then
        raise exception 'contract_id must not be null' using errcode = 'null_value_not_allowed';
    end if;

    if channel_id_ is null then
        raise exception 'channel_id must not be null' using errcode = 'null_value_not_allowed';
    end if;

    select sup.*
    from state_updates as sup
    where
        sup.chain_id = chain_id_ and
        sup.contract_id = contract_id_ and
        sup.channel_id = channel_id_
    order by sequence_num desc
    limit 1
    into latest_state;

    return latest_state;
end
$pgsql$;

create function mcy_null_row_to_json(r anyelement)
returns jsonb
language plpgsql as $pgsql$
begin
    if r is null then
        return 'null'::jsonb;
    end if;
    return row_to_json(r);
end
$pgsql$;
