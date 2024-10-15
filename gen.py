import random

EVENTS_COUNT = 2000000
SESSIONS_COUNT = 50000000
CID_RANGE = (1, 5000001)
DATE_RANGE = (1, 20001)
SESSION_ID_RANGE = (1, 10001)
SESSION_ID_COUNT_RANGE = (1, 201)
VIEW_COUNT_RANGE = (1, 1001)

def gen_events():
    events = {}

    while len(events) < EVENTS_COUNT:
        cid = random.randrange(CID_RANGE[0], CID_RANGE[1])
        if cid not in events:
            events[cid] = random.randrange(DATE_RANGE[0], DATE_RANGE[1])

    return [{"cid": x[0], "eventdate": x[1]} for x in events.items()]

def gen_1_session():
    cid = random.randrange(CID_RANGE[0], CID_RANGE[1])
    session_date = random.randrange(DATE_RANGE[0], DATE_RANGE[1])
    session_count = random.randrange(SESSION_ID_COUNT_RANGE[0], SESSION_ID_COUNT_RANGE[1])
    session_ids = [random.randrange(SESSION_ID_RANGE[0], SESSION_ID_RANGE[1]) for _ in range(session_count)]
    page_count = random.randrange(VIEW_COUNT_RANGE[0], VIEW_COUNT_RANGE[1])
    view_count = random.randrange(VIEW_COUNT_RANGE[0], VIEW_COUNT_RANGE[1])

    return {
        "cid": cid,
        "sessiondate": session_date,
        "sessionids": session_ids,
        "pagecount": page_count,
        "viewcount": view_count
    }

def gen_sessions():
    return (gen_1_session() for _ in range(SESSIONS_COUNT))

def write_data(filename, data):
    with open(filename, "w") as f:
        for d in data:
            f.write(str(d) + '\n')
    print("%s write complete." % filename)

if __name__ == '__main__':
    events = gen_events()

    write_data("events.json", events)

    sessions = gen_sessions()

    write_data("sessions.json", sessions)