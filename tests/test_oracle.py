from testcontainers.oracle import OracleDbContainer
from dwh_oppfolging.apis.oracle_api_v1 import create_oracle_connection

import oracledb


def test_oracle():
    with OracleDbContainer(
        username="testuser",
        password="testpassword",
        dbname="testdbname"
    ) as oracle:
        with oracledb.connect(
            #user="system",
            #password=oracle.oracle_password,
            service_name=oracle.dbname, # type: ignore
            user=oracle.username, # type: ignore
            password=oracle.password, # type: ignore
            host=oracle.get_container_host_ip(),
            port=oracle.get_exposed_port(oracle.port),
            
        ) as con:
            with con.cursor() as cur:
                assert cur.execute("select 1 from dual").fetchall() == [(1,)]

if __name__ == "__main__":
    test_oracle()
