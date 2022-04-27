create table qstatq if not exists (
    id serial primary key,
    time timestamp with time zone,
    queue string(20),
    jobs_total int,
    enabled logical,
    started logical,
    jobs_transit int,
    jobs_queued int,
    jobs_held int,
    jobs_waiting int,
    jobs_running int,
    jobs_exiting int,
    jobs_begun int,
    total_jobfs int,
    total_mem int,
    total_mpiprocs int,
    total_ncpus int,
    total_nodect int);

grant select on qstatq to grafana_ro;

