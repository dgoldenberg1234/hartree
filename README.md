## Hartree Challenge

Dmitry Goldenberg
April 2023

The challenge description:

Please do the same exercise using two different frameworks.

Framework 1. pandas

Framework 2. apache beam python https://beam.apache.org/documentation/sdks/python/using two input files dataset1 and dataset2.
join dataset1 with dataset2 and generate the below output file

```
legal_entity, counter_party, tier, max(rating by counterparty), sum(value where status=ARAP), sum(value where status=ACCR)
```

Also create a new record to add the total for each of legal_entity, counter_party & tier.

Sample data:

```
legal_entity, counterparty, tier, max(rating by counterparty), sum(value where status=ARAP), sum(value where status=ACCR)
L1,Total, Total, calculated_value, calculated_value,calculated_value
L1, C1, Total,calculated_value, calculated_value,calculated_value
Total,C1,Total,calculated_value, calculated_value,calculated_value
Total,Total,1,calculated_value, calculated_value,calculated_value
L2,Total,Total,calculated_value, calculated_value,calculated_value....
like all other values.
```

where calculated_value in sample data needs to be calculated using the above method.

Pointers for the cube aggregations:

https://www.vertica.com/docs/9.2.x/HTML/Content/Authoring/SQLReferenceManual/Statements/SELECT/CUBEaggregate.htm
section “Levels of CUBE Aggregation”

There is a table with 3 column makes up unique key and other numeric column.

 What we are looking for is some kind of aggregation function on numeric column with all possible permutation of values in 3 unique columns.

Means generating aggregation for below combinations of columns part of unique key:

```
Values in only column 1 keeping value for other column as total
Values in only column2 keeping value for other column as total
Values in only column 3 keeping value for other column as total
Values in column 1 and column 2 keeping value for other column as total
Values in column 1 and column 3 keeping value for other column as total
Values in column 2 and column 3 keeping value for other column as total
Values in column 1, 2 & 3
```

