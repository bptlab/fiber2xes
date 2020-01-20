from opyenxes.factory.XFactory import XFactory
from opyenxes.data_out.XesXmlSerializer import XesXmlSerializer
from multiprocessing import (
    cpu_count,
    Process,
    Lock,
    Manager,
)
from .partition_processor import process_partition_events_to_traces


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

    def create_xes_log_from_traces(traces_of_patients, verbose, event_filter, patients, process_number):
        log = XFactory.create_log()

        processes = []
        if process_number == -1:
            cores = cpu_count() * 2
        else:
            cores = process_number
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


def save_event_log_to_file(log, file_path):
    with open(file_path, "w") as file:
        XesXmlSerializer().serialize(log, file)
