from opyenxes.factory.XFactory import XFactory
from opyenxes.data_out.XesXmlSerializer import XesXmlSerializer
from multiprocessing import (
    cpu_count,
    Process,
    Lock,
    Manager,
)
from .partition_processor import process_partition_events_to_traces
from ..translation import Translation
from ..abstraction import Abstraction


class XESFactory(object):

    def partition_dictionary(input_dict, partitions=2):
        return_list = [dict() for idx in range(partitions)]
        dictionary_index = 0
        for k, v in input_dict.items():
            return_list[dictionary_index][k] = v
            if dictionary_index < partitions - 1:
                dictionary_index += 1
            else:
                dictionary_index = 0
        return return_list


    def create_xes_log_from_traces(traces_of_patients, verbose, remove_unlisted, event_filter, patients):
        log = XFactory.create_log()

        processes = []
        cores = cpu_count() * 2
        return_lock = Lock()
        manager = Manager()
        return_dict = manager.dict()

        partitioned_filtered_events = XESFactory.partition_dictionary(traces_of_patients, cores)

        for process_index in range(0, cores):
            process = Process(target=process_partition_events_to_traces, args=(
                return_lock,
                return_dict,
                process_index,
                partitioned_filtered_events[process_index],
                verbose,
                remove_unlisted,
                event_filter,
                patients,))
            processes.append(process)
            process.start()

        for process in processes:
            process.join()

        flatten = lambda l: [item for sublist in l for item in sublist]
        for entry in flatten(return_dict.values()):
            log.append(entry)

        return log

    def translate_and_abstract_event(event, verbose, remove_unlisted):

        if not Translation.is_known_event(event):
            return None, None, None, None

        event_name, event_type, event_context, event_code = Translation.translate_to_event(
            event, verbose)

        abstract_event_name = Abstraction.get_abstract_event_name(
            event_name, remove_unlisted)

        if abstract_event_name is None:
            return None, event_name, event_context, event_code
        elif not verbose:
            return abstract_event_name, event_name, event_context, event_code

        result = event_type

        if event_context is not None and verbose:
            result += (" (" + event_context + " " + event_code + ")")

        if event_name is not None:
            result += (": " + abstract_event_name)

        return result, event_name, event_context, event_code


def save_event_log_to_file(log, file_path):
    with open(file_path, "w") as file:
        XesXmlSerializer().serialize(log, file)
