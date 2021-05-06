"""
    The module contains the class Translation which is used to translate
    events into text representations of diagnosis, material or procedure
"""

import os
import re
import csv

from typing import (
    Tuple,
    Optional
)

dirname = os.path.dirname(__file__)
DIAGNOSIS_ICD_10_VOCAB_PATH = os.path.join(dirname, "vocab-icd10.csv")
DIAGNOSIS_ICD_9_VOCAB_PATH = os.path.join(dirname, "vocab-icd9.csv")
PROCEDURE_CPT_4_VOCAB_PATH = os.path.join(dirname, "vocab-cpt4.csv")


class Translator():
    """
    Class to translate events into diagnosis, material or procedure events
    """
    csv_reader: dict = {}

    def __init__(self):
        pass

    def translate_to_event(self, event, verbose: bool) -> Tuple[
            Optional[str], str, str, bool, Optional[str], str]:
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
        context_diagnosis_code: str = event.context_diagnosis_code
        context_material_code: str = event.context_material_code
        context_procedure_code: str = event.context_procedure_code
        context_name = event.context_name

        if isinstance(context_name, tuple):
            context_name = context_name[0]

        event_name = None
        event_type = ""

        # For verbose output
        event_context = None
        event_code = ""
        anamnesis = False

        # Identify event type
        if is_event_procedure(event):
            # Event is procedure
            event_type = "PROCEDURE"
            event_code = context_procedure_code
            event_description: str = event.procedure_description

            event_context, translation = self.translate_procedure(
                context_name, context_procedure_code, verbose)

            if translation is not None:
                event_name = translation
            else:
                event_name = event.procedure_description

            if event_name is not None and 'PAIN SCORE' in event_name:
                event_description += ': ' + event.value

            consultation = identify_consultation(event_name)

            if consultation is not None:
                event_name = consultation
                event_type = "CONSULTATION"

            if 'Reported' in event.level3_action_name:
                anamnesis = True

        elif is_event_diagnosis(event):
            # Event is diagnosis
            event_type = "DIAGNOSIS"
            event_code = context_diagnosis_code
            event_description = event.description

            event_context, translation = self.translate_diagnosis(
                context_name, context_diagnosis_code, verbose)

            if translation is not None:
                event_name = translation
            else:
                event_name = event.description

            if 'Reported' in event.level3_action_name:
                anamnesis = True

        elif is_event_material(event):
            # Event is material
            event_type = "MATERIAL"
            event_code = context_material_code
            event_name = event.material_name
            event_description = event.material_name

            event_context, translation = translate_material(
                context_name, verbose)

            if translation is not None:
                event_name = translation
            else:
                event_name = event.material_name

            if 'Reported' in event.level3_action_name:
                anamnesis = True

        return event_name, event_description, event_type, anamnesis, event_context, event_code

    def vocabulary_lookup(self, vocabulary_path: str, search_term: str,
                          search_column: int = 0, target_column: int = 1,
                          delimiter: str = ",") -> Optional[str]:
        """
        Looks for a search term in the vocabulary tables.

        Keyword arguments:
        vocabulary_path -- the path to the vocabulary file
        search_term -- the search term
        search_column -- the search column in which the algorithm should search in (default 0)
        target_column -- the target column index, where the result is taken from (default 1)
        delimiter -- the row delimiter (default ,)
        """
        if vocabulary_path not in self.csv_reader:
            self.csv_reader[vocabulary_path] = list(csv.reader(
                open(vocabulary_path), delimiter=delimiter))
        reader = self.csv_reader[vocabulary_path]
        for row in reader:
            if len(row) > search_column and len(row) > target_column:
                if re.search("^" + search_term + "$", row[search_column], re.IGNORECASE) \
                        is not None:

                    return row[target_column]
        return None

    def translate_icd10(self, code: str) -> Optional[str]:
        """
        Translate an ICD-10 code to its textual representation

        Keyword arguments:
        code -- the ICD-10 code
        """
        return self.vocabulary_lookup(
            vocabulary_path=DIAGNOSIS_ICD_10_VOCAB_PATH,
            search_term=str(code),
            search_column=0,
            target_column=1
        )

    def translate_icd9(self, code) -> Optional[str]:
        """
        Translate an ICD-9 code to its textual representation

        Keyword arguments:
        code -- the ICD-9 code
        """
        return self.vocabulary_lookup(
            vocabulary_path=DIAGNOSIS_ICD_9_VOCAB_PATH,
            search_term=str(code),
            search_column=0,
            target_column=1
        )

    def translate_cpt4(self, code) -> Optional[str]:
        """
        Translate an CPT-4 code to its textual representation

        Keyword arguments:
        code -- the CPT-4 code
        """
        return self.vocabulary_lookup(
            vocabulary_path=PROCEDURE_CPT_4_VOCAB_PATH,
            search_term=str(code),
            search_column=1,
            target_column=2
        )

    def translate_procedure(self, context_name: Optional[str],
                            context_procedure_code: str,
                            verbose: bool) -> Tuple[str, Optional[str]]:
        """
        Translate a procedure by the context name and procedure code to a textual representation

        Keyword arguments:
        context_name -- the context name
        context_procedure_code -- the context code
        verbose -- the flag to print additional information to debug
        """
        event_context = "UNKNOWN"
        translation = None
        if context_name is None:
            print("Unknown Procedure Context")
        elif re.match(r"(?:EPIC )*CPT-4", str(context_name)):
            event_context = "CPT-4"
            translation = self.translate_cpt4(context_procedure_code)
        elif context_name == "ICD-10":
            event_context = "ICD-10"
            translation = self.translate_icd10(context_procedure_code)
        elif context_name == "ICD-9":
            event_context = "ICD-9"
            translation = self.translate_icd9(context_procedure_code)
        elif context_name == "SYSTEM":
            event_context = "SYSTEM"
        elif context_name == "IMO":
            event_context = "IMO"
        elif context_name == "EPIC":
            event_context = "EPIC"
        elif verbose:
            print("Unknown Procedure Context")
            print(context_name)

        return event_context, translation

    def translate_diagnosis(self, context_name: Optional[str],
                            context_diagnosis_code: str,
                            verbose: bool) -> Tuple[str, Optional[str]]:
        """
        Translate a diagnosis by the context name and diagnosis code to a textual representation

        Keyword arguments:
        context_name -- the context name
        context_diagnosis_code -- the context code
        verbose -- the flag to print additional information to debug
        """
        event_context = "UNKNOWN"
        translation = None

        if context_name == "ICD-10":
            event_context = "ICD-10"
            translation = self.translate_icd10(context_diagnosis_code)
        elif context_name == "ICD-9":
            event_context = "ICD-9"
            translation = self.translate_icd9(context_diagnosis_code)
        elif context_name == "SYSTEM":
            event_context = "SYSTEM"
        elif context_name == "IMO":
            event_context = "IMO"
        elif context_name == "EPIC":
            event_context = "EPIC"
        elif verbose:
            print("Unknown Diagnosis Context")
            print(context_name)

        return event_context, translation


def translate_material(context_name: Optional[str], verbose: bool) -> Tuple[str, Optional[str]]:
    """
    Translate a material by the context name and material code to the textual representation

    Keyword arguments:
    context_name -- the context name
    context_material_code -- the context code
    verbose -- the flag to print additional information to debug
    """
    event_context = "UNKNOWN"
    translation = None

    if context_name == "EPIC MEDICATION":
        event_context = "EPIC MEDICATION"
    elif verbose:
        print("Unknown Material Context")
        print(context_name)

    return event_context, translation


def identify_consultation(procedure_description: Optional[str]) -> Optional[str]:
    """
    Identify a consultation event

    Keyword arguments:
    procedure_description -- the procedure description
    """
    if procedure_description is None:
        return None

    result = re.search(
        "^CONSULT TO ", procedure_description, re.IGNORECASE)
    if result is not None:
        return procedure_description[result.end():]
    return None


def is_event_procedure(event) -> bool:
    """
    Checks if the given event is a procedure.

    Keyword arguments:
    event -- the event
    """
    return event.context_procedure_code != "MSDW_NOT APPLICABLE" and \
        event.context_procedure_code != "MSDW_UNKNOWN" and \
        event.context_procedure_code is not None


def is_event_diagnosis(event) -> bool:
    """
    Checks if the given event is a diagnosis.

    Keyword arguments:
    event -- the event
    """
    return event.context_diagnosis_code != "MSDW_NOT APPLICABLE" and \
        event.context_diagnosis_code != "MSDW_UNKNOWN" and \
        event.context_diagnosis_code is not None


def is_event_material(event) -> bool:
    """
    Checks if the given event is a material.

    Keyword arguments:
    event -- the event
    """
    return event.context_material_code != "MSDW_NOT APPLICABLE" and \
        event.context_material_code != "MSDW_UNKNOWN" and \
        event.context_material_code is not None


def is_known_event(event) -> bool:
    """
    Checks if the given event is a procedure, diagnosis or material.

    Keyword arguments:
    event -- the event
    """
    return is_event_procedure(event) or is_event_diagnosis(event) or \
        is_event_material(event)
