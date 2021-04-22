"""
Provides list utility methods
"""


def create_list(object_to_list: object) -> list:
    """
    Create a list containing the passed object

    Keyword arguments:
    object_to_list - the object which should be returned as List
    """
    return [object_to_list]


def return_object(object_to_return: object) -> object:
    """
    Simply return the object for combineByKey()

    Keyword arguments:
    object_to_return - the object which should be returned
    """
    return object_to_return


def merge_lists(list_to_merge1: list, list_to_merge2: list) -> list:
    """
    Merge two lists

    Keyword arguments:
    list_to_merge1 - list to merge
    list_to_merge2 - list to merge
    """
    return list_to_merge1 + list_to_merge2


def add_tuple_to_list(present_list: list, new_element: object) -> list:
    """
    Add an element to an existing list

    Keyword arguments:
    present_list - Existing list
    new_element - element which should be added to the list
    """
    return present_list + [new_element]
