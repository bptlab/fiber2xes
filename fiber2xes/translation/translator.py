"""
    The module contains the class Translation which is used to translate
    events into text representations of diagnosis, amterial or procedure
"""

import os
import re
import csv

dirname = os.path.dirname(__file__)
DIAGNOSIS_ICD_10_VOCAB_PATH = os.path.join(dirname, "vocab-icd10.csv")
DIAGNOSIS_ICD_9_VOCAB_PATH = os.path.join(dirname, "vocab-icd9.csv")
PROCEDURE_CPT_4_VOCAB_PATH = os.path.join(dirname, "vocab-cpt4.csv")


class Translator():
    """
    Class to translate events into diagnosis, material or procedure events
    """
    csv_reader = {}

    def __init__(self):
        pass

    def is_event_procedure(self, event):
        """
        Checks if the given event is a procedure.

        Keyword arguments:
        event -- the event
        """
        return event.context_procedure_code != "MSDW_NOT APPLICABLE" and \
               event.context_procedure_code != "MSDW_UNKNOWN"

    def is_event_diagnosis(self, event):
        """
        Checks if the given event is a diagnosis.

        Keyword arguments:
        event -- the event
        """
        return event.context_diagnosis_code != "MSDW_NOT APPLICABLE" and \
               event.context_diagnosis_code != "MSDW_UNKNOWN"

    def is_event_material(self, event):
        """
        Checks if the given event is a material.

        Keyword arguments:
        event -- the event
        """
        return event.context_material_code != "MSDW_NOT APPLICABLE" and \
               event.context_material_code != "MSDW_UNKNOWN"

    def is_known_event(self, event):
        """
        Checks if the given event is a procedure, diagnosis or material.

        Keyword arguments:
        event -- the event
        """
        return self.is_event_procedure(event) or self.is_event_diagnosis(event) or \
            self.is_event_material(event)

    def translate_to_event(self, event, verbose):
        """
        Determines event type and applies translations and abstractions to it

        encounter_type set
        context_diagnosis_code = MSDW_NOT_APPLICABLE | context_diagnosis_code = MSDW_UNKNOWN
        context_procedure_code set
        ->  procedure is event

        encounter_type set
        context_procedure_code = MSDW_NOT_APPLICABLE
        context_diagnosis_code set
        -> diagnosis is event
        """
        context_diagnosis_code = event.context_diagnosis_code
        context_material_code = event.context_material_code
        context_procedure_code = event.context_procedure_code
        context_names = list(filter(lambda c: c != "SYSTEM",
                                    [event.context_name,
                                     event.context_name_1,
                                     event.context_name_2]))

        event_name = None
        event_type = ""

        # For verbose output
        event_context = None
        event_code = ""
        anamnesis = False

        # Identify event type
        if self.is_event_procedure(event):
            # Event is procedure
            event_type = "PROCEDURE"
            event_code = context_procedure_code
            event_description = event.procedure_description

            event_context, translation = self.translate_procedure(
                context_names, context_procedure_code, verbose)

            if translation is not None:
                event_name = translation
            else:
                event_name = event.procedure_description

            if 'PAIN SCORE' in event_name:
                event_description += ': ' + event.value

            consultation = self.identify_consultation(event_name)

            if consultation is not None:
                event_name = consultation
                event_type = "CONSULTATION"

            if 'Reported' in event.level3_action_name:
                anamnesis = True

        elif self.is_event_diagnosis(event):
            # Event is diagnosis
            event_type = "DIAGNOSIS"
            event_code = context_diagnosis_code
            event_description = event.description

            event_context, translation = self.translate_diagnosis(
                context_names, context_diagnosis_code, verbose)

            if translation is not None:
                event_name = translation
            else:
                event_name = event.description

            if 'Reported' in event.level3_action_name:
                anamnesis = True
            
        elif self.is_event_material(event):
            # Event is material
            event_type = "MATERIAL"
            event_code = context_material_code
            event_name = event.material_name
            event_description = event.material_name

            event_context, translation = self.translate_material(
                context_names, verbose)

            if translation is not None:
                event_name = translation
            else:
                event_name = event.material_name

            if 'Reported' in event.level3_action_name:
                anamnesis = True

        return event_name, event_description, event_type, anamnesis, event_context, event_code

    def vocabulary_lookup(self, vocabulary_path, search_term, search_column=0, target_column=1,
                          delimiter=","):
        """
        Looks for a search term in the vocab tables.

        Keyword arguments:
        vocabulary_path -- the path to the vocab-file
        search_term -- the search term
        search_column -- the search column in which the algorithm should search in (default 0)
        target_column -- the target column index, where the result is taken from (default 1)
        delimiter -- the row delimiter (default ,)
        """
        if vocabulary_path not in self.csv_reader:
            self.csv_reader[vocabulary_path] = csv.reader(
                open(vocabulary_path), delimiter=delimiter)
        reader = self.csv_reader[vocabulary_path]
        for row in reader:
            if len(row) > search_column and len(row) > target_column:
                if re.search("^" + search_term + "$", row[search_column], re.IGNORECASE) \
                    is not None:

                    return row[target_column]
        return None

    def translate_icd10(self, code):
        """
        Translate an ICD-10 code to his textual representation

        Keyword arguments:
        code -- the ICD-10 code
        """
        return self.vocabulary_lookup(
            vocabulary_path=DIAGNOSIS_ICD_10_VOCAB_PATH,
            search_term=str(code),
            search_column=1,
            target_column=2
        )

    def translate_icd9(self, code):
        """
        Translate an ICD-9 code to his textual representation

        Keyword arguments:
        code -- the ICD-9 code
        """
        return self.vocabulary_lookup(
            vocabulary_path=DIAGNOSIS_ICD_9_VOCAB_PATH,
            search_term=str(code),
            search_column=0,
            target_column=1
        )

    def translate_cpt4(self, code):
        """
        Translate an CPT-4 code to his textual representation

        Keyword arguments:
        code -- the CPT-4 code
        """
        return self.vocabulary_lookup(
            vocabulary_path=PROCEDURE_CPT_4_VOCAB_PATH,
            search_term=str(code),
            search_column=1,
            target_column=2
        )

    def translate_procedure(self, context_names, context_procedure_code, verbose):
        """
        Translate an procedure by the context name and procedure code to a textual representation

        Keyword arguments:
        context_names -- the context names
        context_procedure_code -- the context code
        verbose -- the flag to print additional information to debug
        """
        event_context = "UNKNOWN"
        translation = None

        # if context_name.str.contains("(?:EPIC )*CPT-4", regex=True).any():
        #    event_context = "CPT-4"
        #    translation = Translation.translate_cpt4(context_procedure_code)
        if "ICD-10" in context_names:
            event_context = "ICD-10"
            translation = self.translate_icd10(context_procedure_code)
        elif "ICD-9" in context_names:
            event_context = "ICD-9"
            translation = self.translate_icd9(context_procedure_code)
        elif "SYSTEM" in context_names:
            event_context = "SYSTEM"
        elif "IMO" in context_names:
            event_context = "IMO"
        elif "EPIC" in context_names:
            event_context = "EPIC"
        elif verbose:
            print("Unknown Procedure Context")
            for context in context_names:
                print(context)

        return event_context, translation

    def translate_diagnosis(self, context_names, context_diagnosis_code, verbose):
        """
        Translate an diagnosis by the context name and diagnosis code to a textual representation

        Keyword arguments:
        context_names -- the context names
        context_diagnosis_code -- the context code
        verbose -- the flag to print additional information to debug
        """
        event_context = "UNKNOWN"
        translation = None

        if "ICD-10" in context_names:
            event_context = "ICD-10"
            translation = self.translate_icd10(context_diagnosis_code)
        elif "ICD-9" in context_names:
            event_context = "ICD-9"
            translation = self.translate_icd9(context_diagnosis_code)
        elif "SYSTEM" in context_names:
            event_context = "SYSTEM"
        elif "IMO" in context_names:
            event_context = "IMO"
        elif "EPIC" in context_names:
            event_context = "EPIC"
        elif verbose:
            print("Unknown Diagnosis Context")
            for context in context_names:
                print(context)

        return event_context, translation

    def translate_material(self, context_names, verbose):
        """
        Translate an material by the context name and material code to the textual representation

        Keyword arguments:
        context_names -- the context names
        context_material_code -- the context code
        verbose -- the flag to print additional information to debug
        """
        event_context = "UNKNOWN"
        translation = None

        if "EPIC MEDICATION" in context_names:
            event_context = "EPIC MEDICATION"
        elif verbose:
            print("Unknown Material Context")
            for context in context_names:
                print(context)

        return event_context, translation

    def identify_consultation(self, procedure_description):
        """
        Identify a consultation event

        Keyword arguments:
        procedure_description -- the procedure description
        """
        result = re.search(
            "^CONSULT TO ", procedure_description, re.IGNORECASE)
        if result is not None:
            return procedure_description[result.end():]
        return None
