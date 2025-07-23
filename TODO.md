# TODO

No particular order yet, just dumping notes here.

- Might be better to just have save() write to the changelog only, and then
we always just perform a merge? So it's Entity -> save() -> changelog -> tables.
This would ensure the changes are always handled in the same exact way whether
it's sync or save.

- I think it might ultimately be better after all to use manifest files, instead
of list. See https://www.backblaze.com/cloud-storage/transaction-pricing

