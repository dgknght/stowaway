# Datalog
## Where clause sorting

- In datalog queries, the most restrictive where clauses should appear
  earlier and the least restrictive where clauses should appear later.
- The caller can specify hints, which are a list of attributes that
  will be pushed to the front, in the order they are given.
- After this, clauses that reference input variable should appear next
- Finally, the remainder should be sorted based on distance from the
  relationship graph.
