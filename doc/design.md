# Design for the PostgreSQL Virtual Schema adapter

## Scalar Functions:

### `MINUTES_BETWEEN`, `HOURSE_BETWEEN` ...

Exasol supports multiple functions that get the diff of a date in a specific unit. In Exasol `MINUTES_BETWEEN` of two dates that are two hours apart return 120.

PostgreSQL does not have such functions.

**Design**: Do not support these capabilities.

#### Other considered options:

* Use `AGE` and `DATE_PART`:     
  This approach does only return the minutes-part of the interval, not the total minutes.

* Use custom math. We could try to do things like dividing the interval by 60 to get it in minutes. This is however dangerous, especially concerning leap seconds.



