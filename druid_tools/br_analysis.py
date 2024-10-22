import pandas as pd

class Analysis:
    def __init__(self, df):
        self.df = df

    # Descriptive statistics for numeric columns
    def get_descriptive_stats(self):
        return self.df.describe()

    # Get value counts for a particular column (e.g., 'birth_place', 'gender')
    def get_value_counts(self, column):
        return self.df[column].value_counts()

    # Correlation between columns
    def get_correlation_matrix(self):
        return self.df.corr()

    # Distribution of a numerical column
    def get_distribution(self, column):
        return self.df[column].plot.hist()

    # Group by a categorical column and get aggregate stats
    def get_grouped_stats(self, group_col, agg_col, agg_func='mean'):
        return self.df.groupby(group_col)[agg_col].agg(agg_func)

    # Missing data statistics
    def missing_data_stats(self):
        return self.df.isnull().sum()

    # Check for duplicates
    def check_duplicates(self, subset=None):
        return self.df.duplicated(subset=subset).sum()

    # Get summary by birth type
    def birth_type_summary(self):
        return self.df.groupby('Birth_type')['Birth_Reg_ID'].count()

    # Mother’s age impact on birth order
    def mother_age_vs_birth_order(self):
        return self.df.groupby('mother_age_at_birth')['birth_order'].mean()

    # Filter for a particular birth place
    def filter_by_birth_place(self, place):
        return self.df[self.df['birth_place'] == place]


class Completeness:
    def __init__(self, df):
        self.df = df

    # Check completeness of required columns
    def check_column_completeness(self, required_columns):
        completeness = {}
        for col in required_columns:
            completeness[col] = 100 - (self.df[col].isnull().mean() * 100)
        return completeness

    # Overall completeness percentage for all columns
    def overall_completeness(self):
        total_cells = self.df.shape[0] * self.df.shape[1]
        missing_cells = self.df.isnull().sum().sum()
        completeness_percent = (1 - missing_cells / total_cells) * 100
        return completeness_percent

    # Completeness by row (e.g., completeness of individual birth records)
    def row_completeness(self):
        return self.df.notnull().mean(axis=1) * 100

    # Completeness of NIN (National Identification Number)
    def nin_completeness(self):
        return 100 - (self.df['nin'].isnull().mean() * 100)

    # Completeness for fields by gender
    def completeness_by_gender(self):
        return self.df.groupby('gender').apply(lambda x: 100 - (x.isnull().mean() * 100))

    # Check completeness of child-related fields
    def child_completeness(self):
        child_columns = ['child', 'nin_child', 'birth_place']
        return self.check_column_completeness(child_columns)

    # Completeness for parent information
    def parent_completeness(self):
        parent_columns = ['mother', 'father', 'nin_mother', 'nin_father']
        return self.check_column_completeness(parent_columns)


class Fraudulent:
    def __init__(self, df):
        self.df = df

    # Detects duplicate entries based on ID or Certificate Number
    def detect_duplicates(self):
        return self.df[self.df.duplicated(subset='Certificate_No', keep=False)]

    # Detect NIN inconsistencies (e.g., duplicate NINs)
    def detect_duplicate_nin(self):
        return self.df[self.df.duplicated(subset='nin', keep=False)]

    # Detect impossible age discrepancies (e.g., parents too young/old)
    def detect_age_discrepancies(self, min_age_m=13, max_age_m=50, min_age_f=15, max_age_f =60):
        invalid_mothers = self.df[(self.df['mother_age_at_birth'] < min_age_m) | (self.df['mother_age_at_birth'] > max_age_m)]
        invalid_fathers = self.df[(self.df['father_age_at_birth'] < min_age_f) | (self.df['father_age_at_birth'] > max_age_f)]
        return invalid_mothers, invalid_fathers

    # Check for invalid NINs (e.g., null or incorrect format)
    def check_invalid_nin(self):
        return self.df[self.df['nin'].str.len() != 11]

    # Detect fraudulent birth orders (e.g., invalid order numbers)
    def detect_invalid_birth_order(self):
        return self.df[self.df['birth_order'] <= 0]

    # Suspicious records based on high birth frequency (multiple births per day)
    def detect_high_birth_frequency(self):
        return self.df.groupby(['Date_Registerred','Registered_By']).size().sort_values(ascending=False)
    
    def detect_high_registration_frequency(self):
        # Group by 'Registered_By' and count the number of registrations per individual
        return self.df.groupby('Registered_By')['Birth_Reg_ID'].count().sort_values(ascending=False)

    # Suspicious entries with incomplete parent information
    def detect_missing_parent_info(self):
        return self.df[(self.df['mother'].isnull()) | (self.df['father'].isnull())]


class Reports:
    def __init__(self, df):
        self.df = df

    # Generate summary report of key statistics
    def generate_summary_report(self):
        return {
            'Total Registrations': len(self.df),
            'Unique Births': self.df['Birth_Reg_ID'].nunique(),
            'Male Births': (self.df['gender'] == 1).sum(),
            'Female Births': (self.df['gender'] == 2).sum(),
            'Average Mother Age': self.df['mother_age_at_birth'].mean(),
            'Average Father Age': self.df['father_age_at_birth'].mean(),
        }

    # Completeness report for the whole dataset
    def generate_completeness_report(self, completeness_instance):
        column_completeness = completeness_instance.check_column_completeness(self.df.columns)
        overall_completeness = completeness_instance.overall_completeness()
        return {
            'Column Completeness': column_completeness,
            'Overall Completeness': overall_completeness
        }

    # Fraud detection report
    def generate_fraud_report(self, fraud_instance):
        duplicates = fraud_instance.detect_duplicates()
        invalid_ages = fraud_instance.detect_age_discrepancies()
        invalid_nin = fraud_instance.check_invalid_nin()
        return {
            'Duplicate Records': len(duplicates),
            'Invalid Mother Ages': len(invalid_ages[0]),
            'Invalid Father Ages': len(invalid_ages[1]),
            'Invalid NINs': len(invalid_nin),
        }

    # Birth type summary report
    def generate_birth_type_report(self):
        return self.df.groupby('Birth_type')['Birth_Reg_ID'].count()

    # Generate report on the number of births by region
    def generate_births_by_region_report(self):
        return self.df.groupby(['birth_place', 'locality_of_birth'])['Birth_Reg_ID'].count()

    # Report on NIN completeness and fraud detection
    def nin_report(self, completeness_instance, fraud_instance):
        nin_completeness = completeness_instance.nin_completeness()
        duplicate_nins = fraud_instance.detect_duplicate_nin()
        return {
            'NIN Completeness': nin_completeness,
            'Duplicate NINs': len(duplicate_nins),
        }
