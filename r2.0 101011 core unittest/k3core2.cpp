/*
 * Copyright (c) 2010 Siemargl <Xiemargl@gmail.com>
 *
 * K3DBMS is free software; you can redistribute it and/or modify
 * it under the terms of the GNU LGPL v3 license. See License.txt for details.
 *
 * Module:  Core of 2.0 version. Actor multithreading model.
 *
 * Modifications history:
 * 101009   First release (need sleeps)
 * 101011	Remove unneeded workarounds
 */

//#include "stdafx.h"

#include "dlib/sockets.h"
#include "dlib/server.h"
#include "dlib/sockstreambuf.h"
#include "dlib/ref.h"
#include "dlib/threads.h"
#include "dlib/pipe.h"
#include "dlib/misc_api.h"  // for dlib::sleep
#include "dlib/logger.h"
#include "dlib/assert.h"
#include <iostream>

using namespace dlib;
using namespace std;

extern logger _k3dlog;
class k3disk_manager;
extern k3disk_manager  _k3disk_manager;
class k3cache_manager;
extern k3cache_manager _k3cache_manager;


//#include "k3dbms.h"

struct k3dg_job
{
    int     id;       // job id, what to do
    wstring entity;   // defines directory name, relative to diskgroup base path
    wstring fname;    // file name, without any path
    uint64  offset;   // offset in file, where to begin
    uint32  connect;  // connection id
    HANDLE  trans;    // transaction handle (or INVALID, for nontransacted)
    HANDLE  cursor;   // cursor for FF/FN operations
    shared_ptr<int> m_data;  // data buffer
    uint32    m_datalen;
    int     nresult;
};
typedef pipe<k3dg_job>::kernel_1a k3dg_job_pipe;

typedef struct k3dg_job k3job_result;   // temporary equal to

/*
-need in config mappings:
[diskgroups]
dg1=d:\data
default=...
temp=....

[entities]
obj_data=dg1
person=group2
;if no such type - in default
*/
//================================ realisation ==================================

class k3disk_group2 : public thread_pool
/// Proxy class version 2.0 for ONE disk spindle. Parallelizm here, 1-3 threads as configured
/// Gets RW jobs from cache task and work on, then send result back to it
{
public:
    k3disk_group2(int nthreads);
    ~k3disk_group2();
    void    opDisk(const k3dg_job& job, k3job_result& done);
};


k3disk_group2::k3disk_group2(int nthreads)
  :  thread_pool(nthreads)
{
}


k3disk_group2::~k3disk_group2()
{
    wait_for_all_tasks();
    _k3dlog << LTRACE << "k3disk_group2::~k3disk_group2() shutdown OK";
}


void
k3disk_group2::opDisk(const k3dg_job& job, k3job_result& done)
{
    _k3dlog << LTRACE << "k3disk_group2::opDisk() start " << job.id;
    dlib::sleep(1000); // emulate slow operation
    done = job;
    done.nresult = 1;
    _k3dlog << LTRACE << "k3disk_group2::opDisk() ending " << job.id;
}


//================================ realisation ==================================
class k3disk_group2;
typedef shared_ptr_thread_safe<k3disk_group2>   k3disk_group2_ptr;
typedef std::map<wstring, k3disk_group2_ptr>    k3disk_group2_map;

class k3disk_manager : public multithreaded_object
/// single instance. Redirect RW request from cache tasks to right diskgroup
/// On init creates needed diskgroups.
{
public:
    k3disk_manager();
    ~k3disk_manager();
    k3disk_group2&  find_diskgroup(const wstring &name); // returns default, if none
    int do_job(const k3dg_job &job, future<k3job_result> &res);  // 0 - error

    k3dg_job_pipe   m_jobs; // internal tasks
private:
    void thread();  // for internal tasks
    k3disk_group2_map m_entities;  // m_entities["entity_name"] == diskgroup, default entity = ""
};


k3disk_manager::k3disk_manager()
  :  m_jobs(10)
{
#   pragma message("TODO: load num threads & disk config")
    register_thread(*this, &k3disk_manager::thread);

    // load config
    m_entities.insert(make_pair(L"", new k3disk_group2(3)));  // default, must be (1)

    // start threads we registered above
    start();
    _k3dlog << LINFO << "k3disk_manager::k3disk_manager() created";
}


k3disk_manager::~k3disk_manager()
{
    _k3dlog << LINFO << "k3disk_manager::k3disk_manager() wait for job_pipe to be empty";
    // wait for all the jobs to be processed
    m_jobs.wait_until_empty();

    _k3dlog << LINFO << "k3disk_manager::k3disk_manager() job_pipe is empty";

    // now disable the job_pipe.  doing this will cause all calls to
    // job_pipe.dequeue() to return false so our threads will terminate
    m_jobs.disable();

    // now block until all the threads have terminated
    wait();
    _k3dlog << LINFO << "k3disk_manager::k3disk_manager() all threads have ended";
}


k3disk_group2&
k3disk_manager::find_diskgroup(const wstring &name)
{
    k3disk_group2_map::iterator it = m_entities.find(name);
    if (it == m_entities.end()) //if no DG set up, redirect to default
        it = m_entities.find(L"");

    DLIB_CASSERT(it != m_entities.end(), "k3disk_manager::find_diskgroup() no default diskgroup");

    return *it->second.get();
}


void
k3disk_manager::thread()
{
    k3dg_job j;
    // Here we loop on jobs from the job_pipe.
    while (m_jobs.dequeue(j))
    {
        // process our job j in some way.
        _k3dlog << LDEBUG << "k3disk_manager::thread() got job " << j.id;
        /* sorry, no wchar_t printing
        _k3dlog << LTRACE << "m_entity:" << j.m_entity << ", m_fname:" << j.m_fname
                << ", m_offset:" << j.m_offset << ", m_trans:" << j.m_trans
                << ", m_cursor:" << j.m_cursor << ", m_datalen:" << j.m_datalen;
        */

        // do_service_job();

        // sleep for 0.1 seconds, some sort of DDOS defence
        dlib::sleep(100);
    }

    _k3dlog << LINFO << "k3disk_manager::thread() ending";
}


int
k3disk_manager::do_job(const k3dg_job &job, future<k3job_result>& res)
{
    k3disk_group2&  dg = find_diskgroup(job.entity);

    future<k3dg_job> fj = job;

    _k3dlog << LDEBUG << "k3disk_manager::do_job() call opDisk ";
    _k3dlog << LTRACE << "connect:" << job.connect;

    dg.add_task(dg, &k3disk_group2::opDisk, fj, res);

    _k3dlog << LDEBUG << "k3disk_manager::do_job() return from opDisk ";
    int x = res.get().nresult;
    _k3dlog << LDEBUG << "k3disk_manager::do_job() nresult = " << x;

    return  res.get().nresult;   // wait for task ending because of future mature
}


//================================ realisation ==================================
#   pragma message("TODO: remove fake")
class k3bufferblock
//temp fake
{
    char*   data;
    uint32  nlen;
};

class k3entity_cache2
/// stores buffers for all cached blocks for one entity (in one transaction)
{
public:
    k3entity_cache2(const wstring &name);
    ~k3entity_cache2();
    void    opCache(const k3dg_job &job, k3job_result &res);
private:
    wstring     m_name;
    read_write_mutex   m_mutex;
    std::map<uint64, k3bufferblock>  m_map; // or K3KEY
};


k3entity_cache2::k3entity_cache2(const wstring &name)
  :  m_name(name)
{
    _k3dlog << LDEBUG << "k3entity_cache2::k3entity_cache2()";
}


k3entity_cache2::~k3entity_cache2()
{
    _k3dlog << LDEBUG << "k3entity_cache2::~k3entity_cache2()";
}


void
k3entity_cache2::opCache(const k3dg_job &job, k3job_result &res)
{
    bool fnd;

   _k3dlog << LDEBUG << "k3entity_cache2::opCache() start";

    m_mutex.lock_readonly();
    fnd = false;//work_on_cache();
    m_mutex.unlock_readonly();

    if(!fnd)
    {
        _k3dlog << LDEBUG << "k3entity_cache2::opCache() start disk operation " << job.id;

        future<k3job_result> disk_res;
        if (!_k3disk_manager.do_job(job, disk_res))
        {
            _k3dlog << LDEBUG << "k3entity_cache2::opCache() got error";
            // error situation
        };

        _k3dlog << LDEBUG << "k3entity_cache2::opCache() got answer from disk " << disk_res.get().nresult;
        // got answer

        m_mutex.lock();
        //update_cache();
        m_mutex.unlock();
        res = disk_res;
        res.nresult = 3;
    }

    _k3dlog << LTRACE << "k3entity_cache2::opCache() ending";
    res.nresult = 2;
}


//================================ realisation ==================================

typedef shared_ptr<k3entity_cache2> k3entity_cache_ptr;  // cache may be shared between sub-entities
typedef std::map<wstring, k3entity_cache_ptr>   k3trans_cache_map;
typedef pair<uint32, HANDLE> k3trans_id; // <connection_num, transact_handle>

class k3trans_cache
/// entities cache list for 1 transaction, str = entity name
{
public:
    k3trans_cache(uint32 conn_id, HANDLE trans_id);
    ~k3trans_cache();
    k3trans_cache_map  m_entity_caches;
    k3entity_cache2& find_entity_cache(const wstring &name); // creates new, if need
private:
    k3trans_id m_id;
    read_write_mutex   m_mutex;
};


k3trans_cache::k3trans_cache(uint32 conn_id, HANDLE trans_id)
  :  m_id(make_pair(conn_id, trans_id))
{
    _k3dlog << LDEBUG << "k3trans_cache::k3trans_cache()";
}


k3trans_cache::~k3trans_cache()
{
    _k3dlog << LDEBUG << "k3trans_cache::~k3trans_cache()";
}


k3entity_cache2&
k3trans_cache::find_entity_cache(const wstring &name)
{
    m_mutex.lock_readonly();
    k3trans_cache_map::iterator it = m_entity_caches.find(name);
    m_mutex.unlock_readonly();

    if (it == m_entity_caches.end())
    {
        m_mutex.lock();
        k3entity_cache_ptr  newp(new k3entity_cache2(name));
        k3trans_cache_map::value_type   new_el(name, newp);
        m_entity_caches.insert(new_el);
        it = m_entity_caches.find(name);
        m_mutex.unlock();
   }
    return *it->second.get();
}


//================================ realisation ==================================
#   pragma message("TODO: sweep() threaded obj")
typedef shared_ptr<k3trans_cache>   k3trans_cache_ptr;
typedef std::map<k3trans_id, k3trans_cache_ptr> k3buffercache_map;
    /// global cache list, transaction list, str = transaction name

class k3cache_manager : public multithreaded_object
/// single instance. Got jobs from connections, then redirects they to free cache thread
/// returns timeout or data to connection
/// 3. thread_object makes cache sync & sweeping
/// 4. function invalidates caches (by message) and notifying all
{
public:
    k3cache_manager();
    ~k3cache_manager();
    k3dg_job_pipe   m_jobs;     // for admin jobs
    int do_job(const k3dg_job &job, future<k3job_result> &res);  // 0 - error
private:
    k3trans_cache& find_trans_cache(const uint32 conn, const HANDLE trans); // creates new, if need
    void    thread();   // for admin jobs
    thread_pool     m_pool;
    k3buffercache_map   m_trans_caches;
    read_write_mutex   m_mutex;
};


k3cache_manager::k3cache_manager()
  :  m_jobs(10), m_pool(10)
{
    register_thread(*this, &k3cache_manager::thread);
    start();
    _k3dlog << LINFO << "k3cache_manager::k3cache_manager() created";
}


k3cache_manager::~k3cache_manager()
{
    _k3dlog << LINFO << "k3cache_manager::~k3cache_manager() wait for job_pipe to be empty";
    // wait for all the jobs to be processed
    m_jobs.wait_until_empty();

    _k3dlog << LINFO << "k3cache_manager::~k3cache_manager() job_pipe is empty";

    // now disable the job_pipe.  doing this will cause all calls to
    // job_pipe.dequeue() to return false so our threads will terminate
    m_jobs.disable();

    // now block until all the threads have terminated
    m_pool.wait_for_all_tasks();
    wait();     // stop ourself

    _k3dlog << LINFO << "k3cache_manager::~k3cache_manager() all threads have ended";
}


k3trans_cache&
k3cache_manager::find_trans_cache(const uint32 conn, const HANDLE trans)
{
    m_mutex.lock_readonly();
    k3trans_id  tid(conn, trans);
    k3buffercache_map::iterator it = m_trans_caches.find(tid);
    m_mutex.unlock_readonly();

    if (it == m_trans_caches.end())
    {
        m_mutex.lock();
        k3trans_cache_ptr el_ptr(new k3trans_cache(conn, trans));
        k3buffercache_map::value_type   new_el(tid, el_ptr);
        m_trans_caches.insert(new_el);
        it = m_trans_caches.find(tid);
        m_mutex.unlock();
    }
    return *it->second.get();
}


void
k3cache_manager::thread()
{
    k3dg_job j;
    // Here we loop on jobs from the job_pipe.
    while (m_jobs.dequeue(j))
    {
        // process our job j in some way.
        _k3dlog << LDEBUG << "k3cache_manager::thread() got job " << j.id;

        // work_on_admin_jobs

        // sleep for 0.1 seconds, some sort of DDOS defence
        dlib::sleep(100);
    }
    _k3dlog << LINFO << "k3cache_manager::thread() thread ending";
}


int
k3cache_manager::do_job(const k3dg_job &job, future<k3job_result>& res)
{
    k3trans_cache &tc = find_trans_cache(job.connect, job.trans);
    k3entity_cache2 &ec = tc.find_entity_cache(job.entity);

    _k3dlog << LDEBUG << "k3cache_manager::do_job() start cache operation";

    future<k3dg_job> fj = job;
    m_pool.add_task(ec, &k3entity_cache2::opCache, fj, res);

    return  res.get().nresult;   // wait for task ending because of future mature
}


//================================ realisation ==================================
class k3connection
/// local connection dumb
{
    uint32  m_id;
public:
    k3connection(uint32 id) : m_id(id)
    {
        _k3dlog << LDEBUG << "k3connection::k3connection() init = " << m_id;
    };
    ~k3connection() {};
    void    opRequest();
};


void
k3connection::opRequest()
{
static int job_n;
    k3dg_job job;
    job.id = ++job_n;
    job.connect = m_id;
    job.entity = L"TEST";

    future<k3job_result> res;

    _k3dlog << LDEBUG << "k3connection::opRequest() send job " << job.id;
    if (!_k3cache_manager.do_job(job, res))
    {
        _k3dlog << LDEBUG << "k3connection::opRequest() got error";

    };

    _k3dlog << LINFO << "k3connection::opRequest() result = " << res.get().nresult << " connect &" << this;
}


logger _k3dlog("main.log");
k3disk_manager  _k3disk_manager;
k3cache_manager _k3cache_manager;


class server_core_test : public multithreaded_object
{
public:
    server_core_test()
    {
        register_thread(*this, &server_core_test::thread);
        register_thread(*this, &server_core_test::thread);
        register_thread(*this, &server_core_test::thread_admin);
        start();
    }
    ~server_core_test()
    {
        stop();
        wait();
    }
    void    thread()
    {
        k3connection conn(uint32(this));
        while (!should_stop())
        {
            conn.opRequest();
            dlib::sleep(250);
        }
    }
    void    thread_admin() // emulate admin requests
    {
        while (!should_stop())
        {
            k3dg_job job;
            job.id = 5555;
            _k3cache_manager.m_jobs.enqueue(job);

            dlib::sleep(500);
        }
    }
};


int main()
{
    _k3dlog.set_level(LALL);
    //_k3dlog.set_output_stream(wcout);
    server_core_test test;
    test.start();
    dlib::sleep(100);  // wait to run all

    cout << "Press enter to end this program" << endl;
    std::cin.get();
}




/*
unittest
//{
Сейчас в 1.0
1. k3trans	    trans1(L"transact1");  // создали транзакцию (опционально)
2. k3entity	ent_objs(&trans1, L"ACID", k3acidobj());  // создали объект "ACID" - множество объектов определенного типа (k3acidobj)

3. trans1.start();   // начали транзакцию
4. k3acidobj obj;  // создали или получили новый объект
5. ent_objs.load(key, ovector, L"AGE"); // загрузили массив объектов по неуникальному ключу
6. ent_objs.save(obj); // добавили/обновили его во множество

7. trans1.commit();

Вариант 2.0 Логичнее ли ?

1. k3trans	    trans1(L"transact1");  // создали транзакцию (опционально)

3. trans1.start();   // начали транзакцию
4. k3acidobj obj;  // создали или получили новый объект. Тип сам знает, где он хранится
5. trans1.load(k3acidobj(), key, ovector, L"AGE"); // загрузили массив объектов определенного типа по неуникальному ключу
6. trans1.save(obj); // добавили/обновили его (сам знает куда) в рамках транзакции

//?? а как с nontransacted ?  nontransacted.load() nontransacted always bound to default connection
k3acidobj obj2;
obj2.load(); // loads from nontransacted
obj2.load(trans1); // loads from trans1
nontransacted.load(k3acidobj(), key, ovector, L"AGE");

7. trans1.commit();    // etc



k3connect   conn("server", "user", "password", default);  // establish remote connection
k3trans	    trans1(L"transact1");  // bind to default connection
k3entity	ent_objs(&trans1, L"ACID", k3acidobj());

ent_objs.drop();  // send job @DROP@ to k3trans
// local client - call conn_manager.pipe.enqueue(), remote - transport to server receiving thread and wait

trans1.start();   // send job @BEGINTRANS@...
k3acidobj obj;
ent_objs.save(obj); // send job @SAVE@BSON@
// internal call updateDepencies for all open indexes
// !!!! no need in real sending, when not commited  ????

ent_objs.forEachKeyRange(150, 186, printFunctor, 0, L"SERNUM");  ???? change logic to iterators/cursors


trans1.commit();    // etc
// send all pending data

//}

*/
