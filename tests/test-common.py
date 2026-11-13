import ck_common as c

def test_run_task():
    assert c.run_task("sleep 0")[0] == 0
    assert c.run_task("foo 3")[0] == 1
    assert c.run_task("sleep -5")[0] == 1

def test_messages():
    m = c.decode(c.encode_result("r", "id1", 0, "small1/t0", 1.0, 2.0, 1))
    assert m["id"] == "id1" and m["attempts"] == 1

def test_ids_unique():
    cid = c.client_id()
    assert c.task_id(cid, 1) != c.task_id(cid, 2)
