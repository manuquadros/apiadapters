from lxml import etree as et


def is_scanned(body: str) -> bool:
    """Check if the content of `body` is just links to scanned-pages"""
    xml = et.XML(body)

    return (
        len(xml) == 1 and et.QName(xml[0]).localname == "supplementary-material"
    )
