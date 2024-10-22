import pandas as pd

# Birth registratoin Data function for creating dictionary mappings

def get_birth_type_dict():
    birth_type_df = pd.DataFrame({
        'Birth_Type_ID': [1, 2, 3, 4, 5, 6, 7],
        'Description': ['Single', 'Twins', 'Triplets', 'Quadruplets', 'Quintuplets', 'Sextuplets', 'Higher']
    })
    return dict(zip(birth_type_df['Birth_Type_ID'], birth_type_df['Description']))

def get_education_dict():
    education_df = pd.DataFrame({
        'Education_ID': [1, 10, 11, 2, 3, 4, 5, 6, 7, 8, 9],
        'Description': ['Koranic', 'Doctorate', 'No Education', 'First school leaving certificate', 'SSCE', 
                        'NCE', 'OND', 'HND', 'Bachelors Degree', 'Post Graduate', 'Masters']
    })
    return dict(zip(education_df['Education_ID'], education_df['Description']))

def get_gender_dict():
    gender_df = pd.DataFrame({
        'Gender_ID': [1, 2],
        'gender': ['Male', 'Female']
    })
    return dict(zip(gender_df['Gender_ID'], gender_df['gender']))

def get_literacy_level_dict():
    literacy_level_df = pd.DataFrame({
        'Literacy_Level_ID': [1, 2],
        'Literacy': ['Literate', 'Illiterate']
    })
    return dict(zip(literacy_level_df['Literacy_Level_ID'], literacy_level_df['Literacy']))

def get_marital_status_dict():
    marital_status_df = pd.DataFrame({
        'Marital_Status_ID': [1, 2, 3, 4, 5],
        'Status_Desc': ['Married', 'Single', 'Widowed', 'Divorced', 'Separated']
    })
    return dict(zip(marital_status_df['Marital_Status_ID'], marital_status_df['Status_Desc']))

def get_relationship_dict():
    relationship_df = pd.DataFrame({
        'Relationship_ID': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
        'Description': ['father', 'mother', 'Brother', 'Sister', 'Grand Parents', 'Paternal Grand Parents', 
                        'Maternal Grand Parents', 'Great Grand Parents', 'Uncle(father Brother)', 
                        'Uncle (mother Brother)', 'Aunt (father Sister)', 'Aunt (mother Sister)', 'Others']
    })
    return dict(zip(relationship_df['Relationship_ID'], relationship_df['Description']))


def get_cadre_dict():
    cadre_df = pd.DataFrame({
        'cadre_id':['C','S','D','E','O'],
        'cadre':['Clerical','Secretariat','Driver','Executive','Officer']
    })