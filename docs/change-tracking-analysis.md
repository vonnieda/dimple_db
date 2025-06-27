# Change Tracking Analysis: Entity vs Attribute Level

## Current Implementation: Entity-Level Tracking

Dimple Data currently tracks changes at the **entity level**, not the attribute level.

### How It Works

When you update an entity, the system stores:
- `old_values`: Complete JSON serialization of the entire entity before change
- `new_values`: Complete JSON serialization of the entire entity after change

Example from `src/db/core.rs`:
```rust
// Capture old values for change tracking
let old_values = if exists {
    Some(self.get_record_as_json(&tx, &table_name, &key_value)?)
} else {
    None
};

// Record change
let new_values = serde_json::to_string(&entity_json)?;
self.record_change(
    &tx,
    &table_name,
    &key_value,
    change_type,
    old_values,
    Some(new_values),
)?;
```

### Storage Format

The `_change` table stores complete entity states:
```sql
CREATE TABLE _change (
    id TEXT NOT NULL PRIMARY KEY,
    transaction_id TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_key TEXT NOT NULL,
    change_type TEXT NOT NULL,
    old_values TEXT,  -- Full JSON of entity before change
    new_values TEXT   -- Full JSON of entity after change
);
```

### Example

If you update just a user's email:
```rust
// Before: {"key": "user-123", "name": "John", "email": "old@example.com", "age": 30}
// After:  {"key": "user-123", "name": "John", "email": "new@example.com", "age": 30}
```

The change record stores both complete objects, not just `{"email": {"old": "old@example.com", "new": "new@example.com"}}`.

## Pros of Entity-Level Tracking

1. **Simpler implementation** - No complex diff logic needed
2. **Easy rollback** - Can restore any previous state completely
3. **Works well with serde** - Natural fit with JSON serialization
4. **Complete audit trail** - Full state at each point in time
5. **No schema dependencies** - Doesn't need to understand entity structure

## Cons of Entity-Level Tracking

1. **Storage overhead** - Full entity duplicated per change
2. **Network overhead** - Larger sync payloads
3. **Can't query specific field changes** - Hard to answer "who changed the email field?"
4. **Merge conflicts** - Harder to merge concurrent changes to different fields
5. **Performance** - More data to serialize/deserialize

## Alternative: Attribute-Level Tracking

Would store only changed fields:
```json
{
  "entity_key": "user-123",
  "changes": {
    "email": {
      "old": "old@example.com",
      "new": "new@example.com"
    }
  }
}
```

### Benefits
- Less storage space
- More efficient sync
- Field-level conflict resolution
- Better change analytics

### Drawbacks
- Complex diff implementation
- Need to handle nested objects
- Schema evolution challenges
- Reconstructing full state requires replaying changes

## Hybrid Approach Considerations

Some systems use a hybrid:
- Store full snapshots periodically
- Store field-level changes between snapshots
- Balance storage vs computation

## Implications for Dimple Data

Current entity-level approach aligns with:
- The "lightweight" philosophy
- Simple conflict resolution (last-write-wins)
- Easy implementation and debugging

Consider attribute-level tracking if:
- Storage becomes a bottleneck
- Need field-level merge capabilities
- Want detailed change analytics
- Large entities with small frequent changes

## Future Enhancement Ideas

1. **Compressed changes** - Store binary diff instead of full JSON
2. **Configurable tracking** - Per-table choice of entity vs attribute
3. **Change views** - Computed views showing field-level changes
4. **Snapshot intervals** - Periodic full snapshots with deltas between