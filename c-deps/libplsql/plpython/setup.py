from distutils.core import setup, Extension
MOD = 'Pythonbepi'
setup(
    name=MOD,
    ext_modules=[
        Extension(
            MOD,
            sources=['../mmg/mcxt.c',
                     '../mmg/aset.c',
                     '../znbase/znbase.c',
                     '../znbase/expr.c',
                     '../utils/elog.c',
                     '../utils/list.c',
                     '../utils/nodes.c',
                     '../utils/string.c',
                     '../utils/stringinfo.c',
                     '../utils/hashmap.c',
                     '../plsql/pl_comp.c',
                     '../plsql/pl_exec.c',
                     '../plsql/pl_handler.c',
                     '../plsql/pl_scanner.c',
                     '../plsql/pl_funcs.c',
                     './python_depi.c',
                     '../parser/pl_gram.tab.c',
                     '../parser/scan.c',
                     '../parser/kwlookup.c',
                     '../parser/scansup.c',
                     '../utils/decoding.c',
                     '../cson/cJSON.c',
                     './plpy_result.c',
                     './plpy_type.c'],
            # sources=['./python_depi.c', '../cson/cJSON.c'],
            extra_compile_args=['-Wall', '-g', '-O0', '-fPIC', '-export-dynamic'],
            include_dirs = ['../include']
        )
    ]
)
