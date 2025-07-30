# TODO

No particular order yet, just dumping notes here.

- Might be better to just have save() write to the changelog only, and then
we always just perform a merge? So it's Entity -> save() -> changelog -> tables.
This would ensure the changes are always handled in the same exact way whether
it's sync or save.

- I think it might ultimately be better after all to use manifest files, instead
of list. See https://www.backblaze.com/cloud-storage/transaction-pricing



Okay the stack is getting pretty deep here, let's TODO:

1. Working on switching dimple_core to dimple_db. I reached the point
where several things were failing due to a u64 serialization error. And
that turned out to be because serde_json::Value does not have
a way to store an array of bytes.

2. Need to stop using serde_json::Value as our intermediary type if it
doesn't actually support our values. Had that feeling for a while,
so need to find something else. serde_value might work. The main
requirement is that I need to be able to set the id. Although now
that I think of it, I can probably do that later directly in the params.

I have just discovered https://docs.rs/serde_bytes/latest/serde_bytes/
and it sounds like it might work with that. And Claude had other
suggestions: https://claude.ai/chat/9009b669-2233-4079-8962-ed775723cad2

    1. serde_bytes with serde_json::Value
    2. rmpv::Value (MessagePack)
    3. serde_cbor::Value
    4. Custom Enum Type

Okay, tried a bunch of those and nothing worked except cbor with serde_bytes
but that is clumsy, so no.

    1. One idea: Type alias for BTreeMap<String, rusqlite::Value> can be the IT
    and then I'll just drop serde_rusqlite entirely.
    2. Another: Convert to params immediately, and just use that as IT.

3. So started to look at everywhere we're using serde_json::Value and
figured best to start by merging / removing save_dynamic vs.
save_internal. And their code differs by quite a bit, so was comparing
those.

