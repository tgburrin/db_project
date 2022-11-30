## db project
The purpose of this project is a very simple database system based very roughly on a proprietary one that I used to work with.
The original version of this was done as a hack week project and the first release version contains that work (about 2 weeks worth of work).

Improvements that could be added include:
- Replaying a journal against the most recent, persisted tables.  A recovery mechanism after a crash is a good idea :)
- Handling cases where the definition of a table changes from the on-disk representation
- A threaded model for handling client connections rather than an fdset and polling
- A simplistic locking mechanism
