census_tools.py Documentation
=============================

This module provides tools to construct both abridged and complete life tables based on given mortality rates.

Classes
-------

- **AbridgedLifeTable**
  
  Constructs an abridged life table using age intervals and their corresponding mortality rates.

- **CompleteLifeTable**
  
  Constructs a complete life table using single-year ages and their corresponding mortality rates.

AbridgedLifeTable
-----------------

**Attributes**:

- age_intervals: List of age intervals (e.g., ['0', '1-4', '5-9']).
- death_rates: List of corresponding mortality rates for age intervals.
- cohort_size: Size of hypothetical cohort (default is 100,000).

**Methods**:

- `calculate_nqx(n, nK=2.5)`: Calculate probability of dying within age intervals.
- `calculate_ndx()`: Calculate number of deaths within age intervals.
- `calculate_lx_nLx(n, nK=2.5)`: Calculate number of survivors and person-years lived within age intervals.
- `calculate_Tx_ex()`: Calculate total person-years lived above age x and life expectancy.
- `construct_life_table(n, nK=2.5)`: Construct the entire abridged life table.

**Sample Usage**:

.. code-block:: python

   from census_tools import AbridgedLifeTable

   age_intervals = ['0', '1-4', '5-9']
   death_rates = [0.05, 0.01, 0.005]

   table_constructor = AbridgedLifeTable(age_intervals, death_rates)
   life_table = table_constructor.construct_life_table(n=5)

   print(life_table)

CompleteLifeTable
-----------------

**Attributes**:

- ages: List of single-year ages (e.g., [0, 1, 2, ...]).
- death_rates: List of corresponding mortality rates for ages.
- cohort_size: Size of hypothetical cohort (default is 100,000).

**Methods**:

- `calculate_qx()`: Calculate probability of dying between age x and x+1.
- `calculate_dx()`: Calculate number of deaths between age x and x+1.
- `calculate_lx_Lx()`: Calculate number of survivors and person-years lived between age x and x+1.
- `calculate_Tx_ex()`: Calculate total person-years lived after age x and life expectancy.
- `construct_life_table()`: Construct the entire complete life table.

**Sample Usage**:

.. code-block:: python

   from census_tools import CompleteLifeTable

   ages = list(range(101))
   death_rates = [0.05] * 5 + [0.01] * 10 + ...  # Sample rates

   table_constructor = CompleteLifeTable(ages, death_rates)
   life_table = table_constructor.construct_life_table()

   print(life_table)

