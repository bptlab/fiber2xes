# fiber2xes

This project contains a python utility intended to use data coming from `fiber` to create `.xes` event logs.
To use this tool you need access to the Mount Sinai Data Warehouse.

## Installation

Follow these steps to install `fiber2xes`:

1. Install fiber according to their [installation guide](https://gitlab.hpi.de/fiber/fiber).
2. Install Spark according to their [installation guide](TODO: LINK).
3. Run the pip installation to install `fiber2xes`:

```
pip install git+https://gitlab.hpi.de/pm1920/fiber2xes.git
```

## Example

TODO: Explain how to run the example and what will be returned.

## Interface

The package offers two methods for the event log creation and filter for trace and event filtering.
The following chapters contains more details about these methods.

### Log creation

```
cohort_to_event_log(
  cohort,
  trace_type,
  verbose=False,
  remove_unlisted=True,
  remove_duplicates=True,
  event_filter=None,
  trace_filter=None,
  cores=multiprocessing.cpu_count(),
  window_size=500,
  abstraction_path=None,
  abstraction_exact_match=False,
  abstraction_delimiter=";"
)
```

Parameters:

- **cohort**: The fiber cohort with the patient
- **trace_type**:
- **verbose=False**:
- **remove_unlisted=True**:
- **remove_duplicates=True**:
- **event_filter=None**:
- **trace_filter=None**:
- **cores=multiprocessing.cpu_count()**:
- **window_size=500**:
- **abstraction_path=None**:
- **abstraction_exact_match=False**:
- **abstraction_delimiter=";"**: The delimiter of the abstraction table

### Log serialization

```
save_event_log_to_file(log, file_path)
```

Parameters:

- **log**:
- **file_path**:

### Trace and event filtering

#### Diagnosis

```
filter = Diagnosis(diagnosis_code)
```

Parameter:
- **diagnosis_code**:

#### Material

```
filter = Material(material_code)
```

Parameter:
- **material_code**:

#### Procedure

```
filter = Procedure(procedure_code)
```

Parameter:
- **procedure_code**:

#### Time

```
filter = Time(one_event_after=None, one_event_before=None, all_events_after=None, all_events_before=None)
```

Parameters:
- **one_event_after**:
- **one_event_before**:
- **all_events_after**:
- **all_events_before**:

#### Generic

```
filter = Generic(lambda_expression)
```

Parameter:
- **lambda_expression**:

#### And

```
filter = And(filter1, filter2)
```

Parameter:
- **filter1** and **filter2**: Two other trace or event filters which will be aggregated by a logical *and*.

#### Or

```
filter = Or(filter1, filter2)
```

Parameter:
- **filter1** and **filter2**: Two other trace or event filters which will be aggregated by a logical *or*.

#### Not

```
filter = Not(filter)
```

Parameter:
- **filter**: The result of the given filter will be negated.

## Contribution
