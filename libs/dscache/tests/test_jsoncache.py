import pytest
from uuid import UUID
from odc.dscache import JsonBlobCache
from odc.dscache._jsoncache import doc2bytes


def gen_test_docs(n, uuid_offset=0):
    def doc(i):
        return {"id": str(UUID(int=i + uuid_offset)), "data": i}

    for i in range(n):
        yield doc(i)


def test_create(tmpdir):
    docs = list(gen_test_docs(100, 0xABCFF00))

    doc_map = {UUID(doc["id"]): doc for doc in docs}

    zdict = JsonBlobCache.train_dictionary(docs, 1024)
    assert isinstance(zdict, bytes)
    assert len(zdict) == 1024

    path = str(tmpdir / "file.db")
    db = JsonBlobCache.create(path, zdict=zdict, truncate=True)
    assert db.path.exists()
    assert db.path.is_file()
    assert str(db.path) == path
    assert db.count == 0
    assert JsonBlobCache.exists(path)

    db.bulk_save(docs)
    assert db.count == len(docs)
    db.close()

    db = JsonBlobCache.open_ro(path)
    assert db.count == len(docs)

    for doc in docs:
        doc_ = db.get(doc["id"])
        assert doc_ == doc

        doc_ = db.get(UUID(doc["id"]))
        assert doc_ == doc

    for _id, doc in db.get_all():
        assert _id in doc_map
        assert doc_map[_id] == doc


def test_doc2bytes_badinputs():
    for doc in ({}, {"id": []}, ([], {})):
        with pytest.raises(ValueError):
            doc2bytes(doc)
