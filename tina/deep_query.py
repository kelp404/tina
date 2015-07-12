import logging
from .properties import ReferenceProperty


def update_reference_properties(documents):
    """
    Update documents for reference property.
    :param documents: {list} [{Document}]
    :return:
    """
    if not len(documents):
        return
    data_table = {}  # {document_class: {document_id: {Document}}}
    reference_properties = []  # all reference properties in this Document

    # scan what kind of documents should be fetched
    for property_name, property in documents[0]._properties.items():
        if not isinstance(property, ReferenceProperty):
            continue
        if property.reference_class not in data_table:
            data_table[property.reference_class] = {}
        reference_properties.append(property)

    # scan what id of documents should be fetched
    for document in documents:
        for property in reference_properties:  # loop all reference properties in the document
            document_id = getattr(document, property.name)
            if document_id:
                data_table[property.reference_class][document_id] = None

    # fetch documents
    for document_class, items in data_table.items():
        for reference_document in document_class.get(list(items.keys()), fetch_reference=False):
            data_table[document_class][reference_document._id] = reference_document

    # update reference properties of documents
    for document in documents:
        for property in reference_properties:  # loop all reference properties in the document
            reference_document = data_table[property.reference_class].get(getattr(document, property.name))
            if property.required and reference_document is None:
                logging.warning("There are a reference class can't mapping")
                continue
            setattr(document, property.name, reference_document)
