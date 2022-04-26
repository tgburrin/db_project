## db project
The purpose of this project is a very simple database system based very roughly on a proprietary one that I used to work with.
Improvements that could be added include:
- Replaying a journal against the most recent, persisted tables.  A recovery mechanism after a crash is a good idea :)
- Handling cases where the definition of a table changes from the on-disk representation
- Pointing to the field in the table from the index rather than copying the value into the index
- Defining fields generically (fielda = string 32, fieldb = u16)
- Using generic comparirison functions for those fields.  Comparing strings is mostly the same, comparing numbers definitly is.
