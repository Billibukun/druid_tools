import numpy as np
import pandas as pd
from statsmodels.tsa.seasonal import seasonal_decompose

def apply_weights(df, weight_dict):
    """
    Apply weights to specific columns in a DataFrame.

    Parameters:
    - df: DataFrame containing the data
    - weight_dict: Dictionary specifying the weights for each column

    Returns:
    - DataFrame with weighted columns
    """
    weighted_df = df.copy()
    for column, weight in weight_dict.items():
        if column in weighted_df.columns:
            weighted_df[column] = weighted_df[column] * weight
    return weighted_df

def create_weights(data, method='equal', custom_fn=None):
    """Creates weights for a data series.

    Args:
        data: A NumPy array containing the data series.
        method: The method used to create the weights. Valid methods are:
            'equal': Creates equal weights for all data points.
            'inverse_variance': Creates weights that are inversely proportional to the variance of the data points.
            'custom': Creates weights using a custom method.
        custom_fn: A function for generating weights when method is 'custom'.

    Returns:
        A NumPy array containing the weights.
    """
    
    # Check for empty data or zero variance
    if len(data) == 0:
        raise ValueError("Data array is empty.")
    if np.var(data) == 0:
        raise ValueError("Variance of data is zero.")
    
    if method == 'equal':
        weights = np.ones(len(data)) / len(data)
    elif method == 'inverse_variance':
        weights = np.ones(len(data)) * (1 / np.var(data))
        weights /= np.sum(weights)  # Normalize to sum to 1
    elif method == 'custom':
        if custom_fn is None:
            raise ValueError("Custom function must be provided for custom method.")
        weights = custom_fn(data)
    else:
        raise ValueError(f'Invalid method: {method}')

    return weights

def simple_moving_average(data, window_size):
  """Calculates the simple moving average of a data series.

  Args:
    data: A NumPy array containing the data series.
    window_size: The size of the moving window.

  Returns:
    A NumPy array containing the smoothed data series.
  """

  smoothed_data = np.zeros(len(data))
  for i in range(window_size, len(data)):
    smoothed_data[i] = np.mean(data[i - window_size:i])
  return smoothed_data


def weighted_moving_average(data, weights):
  """Calculates the weighted moving average of a data series.

  Args:
    data: A NumPy array containing the data series.
    weights: A NumPy array containing the weights for each data point.

  Returns:
    A NumPy array containing the smoothed data series.
  """

  smoothed_data = np.zeros(len(data))
  for i in range(len(data)):
    smoothed_data[i] = np.sum(data[i - len(weights) + 1:i + 1] * weights) / np.sum(weights)
  return smoothed_data

import numpy as np

class ExponentialSmoother:
  """An exponential smoother for smoothing data series.

  Args:
    alpha: The smoothing parameter (a value between 0 and 1).
  """

  def __init__(self, alpha):
    self.alpha = alpha
    self.smoothed_value = None

  def smooth(self, data_point):
    if self.smoothed_value is None:
      self.smoothed_value = data_point
    else:
      self.smoothed_value = self.alpha * data_point + (1 - self.alpha) * self.smoothed_value
    return self.smoothed_value


def seasonal_adjust(data, period):
  """Seasonally adjusts a data series.

  Args:
    data: A NumPy array containing the data series.
    period: The period of the seasonality (e.g., 12 for monthly data).

  Returns:
    A NumPy array containing the seasonally adjusted data series.
  """

  decomposition = seasonal_decompose(data, period=period)
  seasonally_adjusted_data = decomposition.trend + decomposition.resid
  return seasonally_adjusted_data


class AbridgedLifeTable:
    def __init__(self, age_intervals, death_rates, cohort_size=100000):
        self.age_intervals = age_intervals
        self.death_rates = death_rates
        self.cohort_size = cohort_size
        self.life_table = pd.DataFrame({
            'Age_Interval': self.age_intervals,
            'Death_Rate': self.death_rates
        })

    def calculate_nqx(self, n, nK=2.5):
        """Calculate probability of dying within the age interval."""
        self.life_table['nqx'] = (n * self.life_table['Death_Rate']) / (1 + (n - nK) * self.life_table['Death_Rate'])

    def calculate_ndx(self):
        """Calculate number of deaths within the age interval."""
        if 'lx' not in self.life_table.columns:
            self.life_table['lx'] = [self.cohort_size] + [0] * (len(self.life_table) - 1)
        self.life_table['ndx'] = self.life_table['lx'] * self.life_table['nqx']

    def calculate_lx_nLx(self, n, nK=2.5):
        """Calculate number of survivors and person-years lived."""
        for i in range(1, len(self.life_table)):
            self.life_table.loc[i, 'lx'] = self.life_table.loc[i-1, 'lx'] - self.life_table.loc[i-1, 'ndx']
        self.life_table['nLx'] = n * self.life_table['lx'] - nK * self.life_table['ndx']

    def calculate_Tx_ex(self):
        """Calculate total person-years lived above age x and life expectancy."""
        self.life_table['Tx'] = self.life_table['nLx'][::-1].cumsum()[::-1]
        self.life_table['ex'] = self.life_table['Tx'] / self.life_table['lx']

    def construct_life_table(self, n, nK=2.5):
        """Construct the entire abridged life table."""
        self.calculate_nqx(n, nK)
        self.calculate_ndx()
        self.calculate_lx_nLx(n, nK)
        self.calculate_Tx_ex()
        return self.life_table


class CompleteLifeTable:
    def __init__(self, ages, death_rates, cohort_size=100000):
        self.ages = ages
        self.death_rates = death_rates
        self.cohort_size = cohort_size
        self.life_table = pd.DataFrame({
            'Age': self.ages,
            'Death_Rate': self.death_rates
        })

    def calculate_qx(self):
        """Calculate probability of dying between age x and x+1."""
        self.life_table['qx'] = self.life_table['Death_Rate'] / (1 + 0.5 * (1 - self.life_table['Death_Rate']))

    def calculate_dx(self):
        """Calculate number of deaths between age x and x+1."""
        if 'lx' not in self.life_table.columns:
            self.life_table['lx'] = [self.cohort_size] + [0] * (len(self.life_table) - 1)
        self.life_table['dx'] = self.life_table['lx'] * self.life_table['qx']

    def calculate_lx_Lx(self):
        """Calculate number of survivors and person-years lived between age x and x+1."""
        for i in range(1, len(self.life_table)):
            self.life_table.loc[i, 'lx'] = self.life_table.loc[i-1, 'lx'] - self.life_table.loc[i-1, 'dx']
        self.life_table['Lx'] = self.life_table['lx'] - 0.5 * self.life_table['dx']

    def calculate_Tx_ex(self):
        """Calculate total person-years lived after age x and life expectancy."""
        self.life_table['Tx'] = self.life_table['Lx'][::-1].cumsum()[::-1]
        self.life_table['ex'] = self.life_table['Tx'] / self.life_table['lx']

    def construct_life_table(self):
        """Construct the entire complete life table."""
        self.calculate_qx()
        self.calculate_dx()
        self.calculate_lx_Lx()
        self.calculate_Tx_ex()
        return self.life_table

def construct_life_table(death_rates, cohort_size=100000):
    life_table = pd.DataFrame({
        'Age': death_rates.index,
        'qx': death_rates.values  # Probability of dying between age x and x+1
    })

    # Compute dx - Number of people dying between age x and x+1
    life_table['dx'] = cohort_size * life_table['qx']

    # Initialize lx (Number of people alive at the beginning of age x)
    life_table['lx'] = cohort_size
    for i in range(1, len(life_table)):
        life_table.loc[i, 'lx'] = life_table.loc[i - 1, 'lx'] - life_table.loc[i - 1, 'dx']

    # Compute Lx (Total person-years lived between age x and x+1)
    life_table['Lx'] = life_table['lx'] - 0.5 * life_table['dx']

    # Compute Tx (Total number of person-years lived after age x)
    life_table['Tx'] = life_table['Lx'][::-1].cumsum()[::-1]

    # Compute ex (Expectation of life at age x)
    life_table['ex'] = life_table['Tx'] / life_table['lx']

    return life_table
