import os
import re
import csv

DIAGNOSIS_ICD_10_VOCAB_PATH = os.path.join(os.path.expanduser("~"), "fiber-to-xes", "translation", "vocab-icd10.csv")
DIAGNOSIS_ICD_9_VOCAB_PATH = os.path.join(os.path.expanduser("~"), "fiber-to-xes", "translation", "vocab-icd9.csv")
PROCEDURE_CPT_4_VOCAB_PATH = os.path.join(os.path.expanduser("~"), "fiber-to-xes", "translation", "vocab-cpt4.csv")

class Translation(object):
    
    csv_reader = {}
    
    def vocabulary_lookup(vocabulary_path, search_term, search_column = 0, target_column = 1, delimiter = ","):
        if vocabulary_path not in Translation.csv_reader:
            Translation.csv_reader[vocabulary_path] = csv.reader(open(vocabulary_path), delimiter=delimiter)
        reader = Translation.csv_reader[vocabulary_path]
        for row in reader:
            if len(row) > search_column and len(row) > target_column:
                if re.search("^" + search_term + "$", row[search_column], re.IGNORECASE) != None:
                    return row[target_column]
        return None
    
    def translate_icd10(code):
        return Translation.vocabulary_lookup(
            vocabulary_path = DIAGNOSIS_ICD_10_VOCAB_PATH, 
            search_term = str(code), 
            search_column = 1, 
            target_column = 2
        )
    
    def translate_icd9(code):
        return Translation.vocabulary_lookup(
            vocabulary_path = DIAGNOSIS_ICD_9_VOCAB_PATH, 
            search_term = str(code), 
            search_column = 0, 
            target_column = 1
        )
    
    def translate_cpt4(code):
        return Translation.vocabulary_lookup(
            vocabulary_path = PROCEDURE_CPT_4_VOCAB_PATH, 
            search_term = str(code), 
            search_column = 1, 
            target_column = 2
        )
    
    def translate_procedure(context_name, context_procedure_code, verbose):
        event_context = "UNKNOWN"
        translation = None

        if context_name.str.contains("(EPIC )*CPT-4", regex=True).any():
            event_context = "CPT-4"
            translation = Translation.translate_cpt4(context_procedure_code)
        elif context_name.str.contains("ICD-10", regex=False).any():
            event_context = "ICD-10"
            translation = Translation.translate_icd10(context_procedure_code)
        elif context_name.str.contains("ICD-9", regex=False).any():
            event_context = "ICD-9"
            translation = Translation.translate_icd9(context_procedure_code)
        elif context_name.str.contains("SYSTEM", regex=False).any():
            event_context = "SYSTEM"
        elif context_name.str.contains("IMO", regex=False).any():
            event_context = "IMO"
        elif context_name.str.contains("EPIC", regex=False).any():
            event_context = "EPIC"
        elif verbose:
            print("Unknown Procedure Context: " + context_name)

        return event_context, translation
    
    def translate_diagnosis(context_name, context_diagnosis_code, verbose):
        event_context = "UNKNOWN"
        translation = None

        if context_name.str.contains("ICD-10", regex=False).any():
            event_context = "ICD-10"
            translation = Translation.translate_icd10(context_diagnosis_code)
        elif context_name.str.contains("ICD-9", regex=False).any():
            event_context = "ICD-9"
            translation = Translation.translate_icd9(context_diagnosis_code)           
        elif context_name.str.contains("SYSTEM", regex=False).any():
            event_context = "SYSTEM"
        elif context_name.str.contains("IMO", regex=False).any():
            event_context = "IMO"
        elif context_name.str.contains("EPIC", regex=False).any():
            event_context = "EPIC"
        elif verbose:
            print("Unknown Diagnosis Context: " + context_name)

        return event_context, translation
    
    def translate_material(context_name, context_material_code, verbose):
        event_context = "UNKNOWN"
        translation = None
        
        if context_name.str.contains("EPIC MEDICATION", regex=False).any():
            event_context = "EPIC MEDICATION"
        elif verbose:
            print("Unknown Material Context: " + context_name)
        
        return event_context, translation