# TODO

No particular order yet, just dumping notes here.

- Might be better to just have save() write to the changelog only, and then
we always just perform a merge? So it's Entity -> save() -> changelog -> tables.
This would ensure the changes are always handled in the same exact way whether
it's sync or save.

- Bug: We're sending notifications on save() even if there were no changes.
We should only send notifications for what actually changes. Probably just
drive it with the changelog.