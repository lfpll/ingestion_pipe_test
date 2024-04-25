
Considerations:
 - For the sake of this is just a test I will not think about complex data types that come from string on csv inference e.g:date, timestamp, ts
- The conversions is not perfect, object type could be converted to numbers in a initial case of nulls
- For simplicity of the code I used pandas types


Improvements

- Adding inference for date, timestamp, ts and relative data types using regexp
- Separate the runner from schema change component
- Add a failure where schema cahnges would be sinked to a table for further analysis
