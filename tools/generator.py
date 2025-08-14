import os
import sys
import re
import shutil
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from dotenv import load_dotenv
load_dotenv("generator.env")

CPP_RETURN_TYPE_MAP = {
    "boolean": "bool",
    "integer": "int32_t",
    "int4": "int32_t",
    "timestamptz": "timestamp_tz_t",
    "float": "double",
    "interval": "interval_t",
    "cstring": "string_t",
    "text": "string_t",
    "bytea": "string_t"
}

EXECUTOR_MAP = {
    1: "UnaryExecutor",
    2: "BinaryExecutor",
    3: "TernaryExecutor"
}

MOBILITYDUCK_CUSTOM_TYPES = [
    # temporal types
    'tint', 'tfloat', 'tbool', 'ttext', 'tbox',
    # span types
    'intspan', 'bigintspan', 'floatspan', 'datespan', 'tstzspan',
    # set types
    'intset', 'bigintset', 'floatset', 'textset', 'dateset', 'tstzset',
    # spanset types
    'intspanset', 'bigintspanset', 'floatspanset', 'textspanset', 'datespanset', 'tstzspanset',
    # geom/geog types
    'tgeometry', 'tgeompoint',
    # generic types
    "temporal", "span", "set", "spanset"
]

PG_TYPE_MAP = {
    "TBOX_P": "TBox *",
    "SPAN_P": "Span *",
    "SET_P": "Set *",
    "BOOL": "bool",
    "INT32": "int32_t",
    "FLOAT8": "double",
    "TIMESTAMPTZ": "timestamp_tz_t",
    "INTERVAL_P": "MeosInterval *",
    "CSTRING": "string_t",
    "TEXT_P": "string_t",
    "BYTEA_P": "string_t"
}

RESERVED_KWS_MAP = {
    "result": "ret"
}

BASETYPE_MAP = {
    "integer": "T_INT4",
    "float": "T_FLOAT8"
}

@dataclass
class SQLFunction:
    """Represents a MobilityDB SQL function"""
    name: str
    return_type: str
    parameters: List[Tuple[str, str]]  # (type, name) pairs
    hash_str: str
    c_function: str
    sql_definition: str
    category: str = "unknown"
    is_operator: bool = False
    operator_symbol: Optional[str] = None
    is_aggregate: bool = False
    is_cast: bool = False

    def __str__(self):
        return f"SQLFunction(name={self.name}\n\treturn_type={self.return_type}\n\tparameters={self.parameters}\n\thash_str={self.hash_str}\n\tc_function={self.c_function}\n\tcategory={self.category}\n\tis_operator={self.is_operator}\n\tis_aggregate={self.is_aggregate}\n\tis_cast={self.is_cast})"

class MobilityDBSQLParser:
    def __init__(self, mobilitydb_sql_dir: str = os.getenv("MOBILITYDB_SQL_DIR")):
        self.mobilitydb_sql_dir = mobilitydb_sql_dir
        self.functions = []
        self.defined_types = []
        self.category_marks = []
        self.cast_definitions = []

    def parse_category_marks(self, content: str):
        """
        Parse the text between comments like:
        /*****************************************************************************
         * Conversion functions
         *****************************************************************************/
        and extract the section name ("Conversion functions") and its line number.

        Returns:
            Dictionary: {section_name: line_number}
        """
        category_marks = {}
        # Regex to match the section header comment
        # The pattern matches a block comment with any number of * and a line with the section name
        pattern = re.compile(
            r'/\*+\s*\n\s*\*\s*([^\n*]+?)\s*\n\s*\*+\s*\*/',
            re.MULTILINE
        )

        for match in pattern.finditer(content):
            section_name = match.group(1).strip()
            section_key = section_name.split()[0].lower()
            # Find the line number in the content
            start_pos = match.start()
            line_number = content[:start_pos].count('\n') + 1
            category_marks[section_key] = line_number

        return category_marks

    def parse_cast_functions(self, content: str):
        if not hasattr(self, "cast_definitions"):
            self.cast_definitions = []
        cast_pattern = re.compile(
            r'CREATE\s+CAST\s*\(\s*(\w+)\s+AS\s+(\w+)\s*\)\s+WITH\s+FUNCTION\s+(\w+)\s*\(([^)]*)\)\s*;',
            re.IGNORECASE
        )
        definitions = []
        for match in cast_pattern.finditer(content):
            source_type = match.group(1)
            target_type = match.group(2)
            function = match.group(3)
            function_args = [arg.strip() for arg in match.group(4).split(",") if arg.strip()]
            definitions.append({
                "source_type": source_type,
                "target_type": target_type,
                "function": function,
                "function_args": function_args
            })

        return definitions

    def parse_sql_file(self, filename: str) -> List[SQLFunction]:
        filepath = os.path.join(self.mobilitydb_sql_dir, filename)
        if not os.path.exists(filepath):
            print(f"Warning: SQL file {filepath} not found")
            return []

        with open(filepath, 'r') as f:
            content = f.read()

        # Parse defined types
        pattern = re.compile(r'CREATE\s+TYPE\s+(\w+);', re.IGNORECASE)
        self.defined_types = pattern.findall(content)

        # Parse category marks
        self.category_marks = self.parse_category_marks(content)

        # Parse cast definitions
        self.cast_definitions = self.parse_cast_functions(content)

        functions = []

        func_pattern = r'CREATE\s+FUNCTION\s+(\w+)\s*\(([^)]*)\)\s*RETURNS\s+(\w+)\s+AS\s+\'[^\']*\',\s*\'([^\']+)\''
        matches = re.finditer(func_pattern, content, re.IGNORECASE | re.DOTALL)

        for match in matches:
            func_name = match.group(1)
            params_str = match.group(2).strip()
            return_type = match.group(3)
            c_function = match.group(4)

            # Get the line number of the CREATE FUNCTION statement
            start_pos = match.start()
            func_line_number = content[:start_pos].count('\n') + 1

            parameters = self._parse_parameters(params_str)
            hash_str = self._generate_hash_str(func_name, parameters)
            category = self._determine_category(func_name, parameters, return_type, func_line_number)
            is_operator = self._is_operator_function(category)
            is_aggregate = self._is_aggregate_function(category)
            is_cast = self._is_cast_function(func_name, parameters)

            sql_func = SQLFunction(
                name=func_name,
                return_type=return_type,
                parameters=parameters,
                hash_str=hash_str,
                c_function=c_function,
                sql_definition=match.group(0),
                category=category,
                is_operator=is_operator,
                is_aggregate=is_aggregate,
                is_cast=is_cast
            )

            functions.append(sql_func)
        return functions

    def _parse_parameters(self, params_str: str) -> List[Tuple[str, str]]:
        if not params_str:
            return []

        parameters = []
        param_parts = self._split_parameters(params_str)

        for param_str in param_parts:
            param_str = param_str.strip()
            if param_str:
                parts = param_str.split()
                if len(parts) >= 1 and "DEFAULT" not in param_str:
                    param_type = parts[0]
                    param_name = parts[1] if len(parts) > 1 else f"arg{len(parameters)}"
                    parameters.append((param_type, param_name))

        return parameters

    def _split_parameters(self, params_str: str) -> List[str]:
        parts = []
        current = ""
        paren_count = 0

        for char in params_str:
            if char == '(':
                paren_count += 1
            elif char == ')':
                paren_count -= 1
            elif char == ',' and paren_count == 0:
                parts.append(current.strip())
                current = ""
                continue
            current += char
        
        if current.strip():
            parts.append(current.strip())

        return parts

    def _generate_hash_str(self, func_name: str, parameters: List[Tuple[str, str]]) -> str:
        return f"{func_name}_{'_'.join([param[0] for param in parameters])}"

    def _determine_category(self, func_name: str, parameters: List[Tuple[str, str]], return_type: str, func_line_number: int) -> str:
        # Determine the category of the function based on its line number and self.category_marks
        # self.category_marks is a dict: {section_name: line_number}
        # Find the section whose line_number is the greatest but <= func_line_number
        if not self.category_marks:
            return "unknown"
        best_category = None
        best_line = -1
        for category, line in self.category_marks.items():
            if line <= func_line_number and line > best_line:
                best_category = category
                best_line = line
        return best_category if best_category is not None else "unknown"

    def _is_operator_function(self, category: str) -> bool:
        return category in ["topological", "position", "set", "comparison"]

    def _is_aggregate_function(self, category: str) -> bool:
        return category in ["extent"]
    
    def _is_cast_function(self, func_name: str, parameters: List[Tuple[str, str]]) -> bool:
        for cast_def in self.cast_definitions:
            if cast_def["function"] == func_name and cast_def["function_args"] == [param[0] for param in parameters]:
                return True
        if func_name.endswith("_in") or func_name.endswith("_out"):
            return True
        return False

@dataclass
class CFunction:
    name: str
    parameters: List[Tuple[str, str]]
    needs_basetype: bool
    meos_func: str
    meos_args: List[str]
    meos_return_type: str
    nullable: bool
    implementation: str

    def __str__(self):
        return f"CFunction(name={self.name}\n\tparameters={self.parameters}\n\tneeds_basetype={self.needs_basetype}\n\tmeos_func={self.meos_func}\n\tmeos_args={self.meos_args}\n\tmeos_return_type={self.meos_return_type}\n\tnullable={self.nullable}\n\timplementation={self.implementation}\n\tdisambiguation_key={self.disambiguation_key})"

class MobilityDBCParser:
    def __init__(self, mobilitydb_src_dir: str = os.getenv("MOBILITYDB_SRC_DIR")):
        self.mobilitydb_src_dir = mobilitydb_src_dir

    def parse_c_file(self, filename: str) -> List[CFunction]:
        filepath = os.path.join(self.mobilitydb_src_dir, filename)
        if not os.path.exists(filepath):
            print(f"Warning: C file {filepath} not found")
            return []

        with open(filepath, 'r') as f:
            content = f.read()

        functions = []
        func_pattern = r'PGDLLEXPORT\s+Datum\s+(\w+)\s*\(PG_FUNCTION_ARGS\);'
        matches = re.finditer(func_pattern, content)

        for match in matches:
            func_name = match.group(1)
            impl_pattern = rf'Datum\s+{func_name}\s*\(PG_FUNCTION_ARGS\)\s*\{{(.*?)\n\}}'
            impl_match = re.search(impl_pattern, content, re.DOTALL)

            if impl_match:
                implementation = impl_match.group(1)
                parameters = self._parse_parameters(implementation)
                needs_basetype = self._needs_basetype(implementation)
                meos_func, meos_args = self._extract_meos_func_args(implementation)
                meos_return_type, nullable = self._extract_meos_return_type(implementation)

            c_func = CFunction(
                name=func_name,
                parameters=parameters,
                needs_basetype=needs_basetype,
                meos_func=meos_func,
                meos_args=meos_args,
                meos_return_type=meos_return_type,
                nullable=nullable,
                implementation=implementation,
            )

            functions.append(c_func)

        return functions

    def _extract_meos_return_type(self, implementation: str) -> Tuple[str, bool]:
        # Detect if PG_RETURN_NULL() exists
        nullable = bool(re.search(r'\bPG_RETURN_NULL\s*\(\s*\)', implementation))
        pattern = re.compile(r'PG_RETURN_(\w+)\s*\(')
        matches = pattern.findall(implementation)
        filtered = [m for m in matches if m != 'NULL']
        return_type = filtered[0] if filtered else None
        return return_type, nullable

    def find_pg_getargs(self, src: str) -> List[Tuple[str, str, int, Optional[str]]]:
        """
        Find PG_GETARG_* occurrences and return tuples:
        (MACRO_SUFFIX, variable_name, index, type_or_cast_if_any)

        - For declarations like:
            Span *span = PG_GETARG_SPAN_P(0);
        returns ('SPAN_P', 'span', 0, 'Span *')

        - For assignments like:
            dbl_dig_for_wkt = PG_GETARG_INT32(1);
        returns ('INT32', 'dbl_dig_for_wkt', 1, None)

        - For casts like:
            StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);
        returns ('POINTER', 'buf', 0, 'StringInfo')
        """
        results = []
        seen_spans = []

        # 1) Declaration pattern: captures a preceding type (contains * and whitespace),
        #    optional cast, macro suffix and index.
        decl_re = re.compile(r'''
            (?P<type>[\w:\s\*]+?)      # type name (e.g. "const char *", "Span *", allow ':' for struct)
            \s+
            (?P<var>[A-Za-z_]\w*)      # variable name
            \s*=\s*
            (?:\(\s*(?P<cast>[^)]+?)\s*\)\s*)?  # optional cast before the macro
            PG_GETARG_(?P<arg>[A-Z0-9_]+)       # macro suffix (UPPER_SNAKE)
            \s*\(\s*(?P<idx>\d+)\s*\)           # index
        ''', re.VERBOSE | re.MULTILINE)

        for m in decl_re.finditer(src):
            arg = m.group('arg')
            var = m.group('var')
            idx = int(m.group('idx'))
            typ = (m.group('type') or '').strip()
            # Prefer explicit cast if present as "type info" for clarity
            cast = m.group('cast')
            type_or_cast = (cast.strip() if cast else typ) if (cast or typ) else None

            results.append((arg, var, idx, type_or_cast))
            seen_spans.append(m.span())

        # 2) Assignment pattern: captures assignments where variable may have been declared earlier.
        assign_re = re.compile(r'''
            (?P<var>[A-Za-z_]\w*)      # variable name
            \s*=\s*
            (?:\(\s*(?P<cast>[^)]+?)\s*\)\s*)?  # optional cast
            PG_GETARG_(?P<arg>[A-Z0-9_]+)
            \s*\(\s*(?P<idx>\d+)\s*\)
        ''', re.VERBOSE | re.MULTILINE)

        for m in assign_re.finditer(src):
            span = m.span()
            # skip overlaps with declaration matches (already captured)
            if any(not (span[1] <= s[0] or span[0] >= s[1]) for s in seen_spans):
                continue
            arg = m.group('arg')
            var = m.group('var')
            idx = int(m.group('idx'))
            cast = m.group('cast')
            type_or_cast = cast.strip() if cast else None
            results.append((arg, var, idx, type_or_cast))

        return results

    def _parse_parameters(self, implementation: str) -> List[Tuple[str, str]]:
        result = self.find_pg_getargs(implementation)
        result = sorted(result, key=lambda x: x[2])
        return result

    def _needs_basetype(self, implementation: str) -> bool:
        return "meosType basetype =" in implementation

    def _parse_meos_func_args(self, implementation):
        lines = [line.strip() for line in implementation.split("\n") if line != ""]
        if 'PG_RETURN_' in lines[-1]:
            return_line = lines[-1]
        else:
            return_line = ""
        if len(return_line.split("(")) > 2:
            func_name, args_str = return_line.split("(")[1:]
            args_str = args_str.replace(")", "").replace(";", "")
            args = args_str.split(",")
            args = [arg.strip() for arg in args]
        else:
            func_name = None
            args = [return_line.split("(")[1].replace(")", "").replace(";", "").strip()]
        return func_name, args

    def _extract_meos_func_args(self, implementation: str) -> Tuple[str, List[str]]:
        func, args = self._parse_meos_func_args(implementation)
        return func, args

@dataclass
class DuckDBTemplate:
    name: str
    template_type: str
    cpp_code: str

class DuckDBTemplateGenerator:
    def __init__(self, struct_name: str = "TemporaryStruct"):
        self.templates = self._build_templates()
        self.struct_name = struct_name

    def _build_templates(self) -> Dict[str, str]:
        return {
            'scalar_func': """void {struct_name}::{function_name}(DataChunk &args, ExpressionState &state, Vector &result) {{
    {implementation}
    if (args.size() == 1) {{
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }}
}}
""",
            'cast_func': """bool {struct_name}::{function_name}(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {{
    bool success = true;
    try {{
        {implementation}
    }} catch (const std::exception &e) {{
        throw InternalException(e.what());
        success = false;
    }}
    return success;
}}
"""
        }

    def generate_function(self, sql_func: SQLFunction, c_func: CFunction, disamg_key: str) -> DuckDBTemplate:
        all_templates = []
        # print(sql_func)
        # print(c_func)
        # If in/out -- only cast
        excluded_categories = ["selectivity", "topological", "position", "set", "extent", "comparison"]
        return_type = sql_func.return_type if sql_func.return_type in MOBILITYDUCK_CUSTOM_TYPES else CPP_RETURN_TYPE_MAP[sql_func.return_type]
        struct_name = self.struct_name
        if sql_func.category not in excluded_categories and not any(token in sql_func.name for token in ["recv", "send"]) and not disamg_key.endswith("_cast"):
            implementation = self._generate_implementation(sql_func, c_func, is_cast=False)
            template = self.templates['scalar_func']
            cpp_code = template.format(
                struct_name=struct_name,
                function_name=disamg_key,
                implementation=implementation
            )
            print(cpp_code)
            all_templates.append(DuckDBTemplate(
                name=sql_func.name,
                template_type="scalar_func",
                cpp_code=cpp_code
            ))

        elif disamg_key.endswith("_cast"):
            implementation = self._generate_implementation(sql_func, c_func, is_cast=True)
            template = self.templates['cast_func']
            cpp_code = template.format(
                struct_name=struct_name,
                function_name=disamg_key,
                implementation=implementation
            )
            print(cpp_code)
            all_templates.append(DuckDBTemplate(
                name=sql_func.name,
                template_type="cast_func",
                cpp_code=cpp_code
            ))
        
        return all_templates

    def _validate_meos_args(self, c_func_args: List[str], c_func_params: List[str]) -> List[str]:
        meos_args = [arg if arg not in RESERVED_KWS_MAP else RESERVED_KWS_MAP[arg] for arg in c_func_args]
        # Check meos_args for timestamp-related variables
        # c_func.parameters=[('SPAN_P', 'span', 0, None), ('TIMESTAMPTZ', 't', 1, 'TimestampTz')]
        # c_func.meos_args=['span', 't']
        for i in range(len(c_func_params)):
            for j in range(len(c_func_args)):
                if c_func_params[i][0] == "TIMESTAMPTZ" and c_func_params[i][1] == c_func_args[j]:
                    meos_args[j] = "(TimestampTz)meos_ts"
        return meos_args

    def _parse_meos_case_2(self, processing_lines):
        line = processing_lines[0]
        line_parts = [part.strip() for part in line.split("=")]
        function = line_parts[1]
        func_name, args_str = function.split("(")
        args_str = args_str.replace(")", "").replace(";", "")
        args = args_str.split(",")
        args = [arg.strip() for arg in args]
        return func_name, args

    def _parse_meos_case_3(self, processing_lines):
        line = processing_lines[1]
        func_name, args_str = line.split("(")
        args_str = args_str.replace(")", "").replace(";", "")
        args = args_str.split(",")
        args = [arg.strip() for arg in args]
        return func_name, args

    def _parse_meos_case_4(self, processing_lines):
        if "=" not in processing_lines[0]:
            # E.g. ['double result;', 'if (! tbox_xmin(box, &result))', 'PG_RETURN_NULL();', 'PG_RETURN_FLOAT8(result);']
            var_dec_line, func_line = processing_lines[:2]
            var_type, var_name = var_dec_line.split(" ")
            var_name = var_name.replace(";", "")
            if var_name in RESERVED_KWS_MAP:
                var_name = RESERVED_KWS_MAP[var_name]
            function = func_line.split("!")[1].strip()
            func_name, args_str = function.split("(")
            args_str = args_str.replace(")", "").replace(";", "")
            args = args_str.split(",")
            args = [arg.strip() for arg in args]
            clean_args = []
            for arg in args:
                if "&" in arg:
                    clean_arg = arg if arg.replace("&", "") not in RESERVED_KWS_MAP else "&" + RESERVED_KWS_MAP[arg.replace("&", "")]
                    clean_args.append(clean_arg)
                else:
                    clean_args.append(arg)
            return var_type, var_name, func_name, clean_args
        else:
            # E.g. ['TBox *result = tbox_expand_value(box, value, basetype);', 'if (! result)', 'PG_RETURN_NULL();', 'PG_RETURN_TBOX_P(result);']
            func_line = processing_lines[0]
            line_parts = [part.strip() for part in func_line.split("=")]
            function = line_parts[1]
            func_name, args_str = function.split("(")
            args_str = args_str.replace(")", "").replace(";", "")
            args = args_str.split(",")
            args = [arg.strip() for arg in args]
            return None, None, func_name, args

    def _parse_meos_case_4_not_nullable(self, processing_lines):
        # e.g. ['uint8_t *wkb = (uint8_t *) VARDATA(bytea_wkb);', 'TBox *result = tbox_from_wkb(wkb, VARSIZE(bytea_wkb) - VARHDRSZ);', 'PG_FREE_IF_COPY(bytea_wkb, 0);', 'PG_RETURN_TBOX_P(result);']
        processing_code = []
        for line in processing_lines:
            clean_line = "\n\t\t\t" + line.replace("result", "ret")
            if "PG_" not in clean_line:
                processing_code.append(clean_line)
        return processing_code

    def _generate_implementation(self, sql_func: SQLFunction, c_func: CFunction, is_cast: bool = False) -> str:
        implementation = ""
        # UnaryExecutor::Execute<string_t, bool>(
        num_args = len(sql_func.parameters)
        executor = EXECUTOR_MAP[num_args]
        implementation += f"{executor}::Execute{'WithNulls' if c_func.nullable else ''}"
        executor_params = []
        for param in sql_func.parameters:
            if param[0] in MOBILITYDUCK_CUSTOM_TYPES:
                executor_params.append("string_t")
            else:
                executor_params.append(CPP_RETURN_TYPE_MAP[param[0]])
        implementation += f"<{', '.join(executor_params)}"
        implementation += f", {'string_t' if sql_func.return_type in MOBILITYDUCK_CUSTOM_TYPES else CPP_RETURN_TYPE_MAP[sql_func.return_type]}>(\n"
        # args.data[0], result, args.size(),
        if not is_cast:
            arg_strs = []
            for i in range(num_args):
                arg_strs.append(f"args.data[{i}]")
            implementation += f"\t\t"
            implementation += f", ".join(arg_strs)
            implementation += f", "
            implementation += f"result, args.size(),\n\t\t[&]("
        else:
            implementation += f"\t\tsource, result, count,\n\t\t[&]("
        # [&](string_t tbox_str) {
        param_strs = []
        for i, param_type in enumerate(executor_params):
            param_strs.append(f"{param_type} args{i}")
        implementation += f", ".join(param_strs)
        implementation += ") {"

        # TBox *tbox = ...
        input_lines = []
        for (param_type, param_name, idx, cast) in c_func.parameters:
            if param_name in RESERVED_KWS_MAP:
                param_name = RESERVED_KWS_MAP[param_name]
            if param_type == 'DATUM':
                cpp_type = "Datum"
                input_template = """
            Datum {param_name} = DataHelpers::getDatum(args{idx});
"""
                input_lines.append(input_template.format(param_name=param_name, idx=idx, func_name=c_func.name))
            else:
                cpp_type = PG_TYPE_MAP[param_type]
                if cpp_type[:-2].lower() in MOBILITYDUCK_CUSTOM_TYPES:
                    input_template = """
            {cpp_type}{param_name} = nullptr;
            if (args{idx}.GetSize() > 0) {{
                {param_name} = ({cpp_type})malloc(args{idx}.GetSize());
                memcpy({param_name}, args{idx}.GetDataUnsafe(), args{idx}.GetSize());
            }}
"""
                    error_template = """\tif (!{param_name}) {{
                throw InternalException("Failure in {func_name}: unable to cast binary to {cpp_type}");
            }}
"""

                    input_lines.append(input_template.format(cpp_type=cpp_type, param_name=param_name, idx=idx, func_name=c_func.name))
                    input_lines.append(error_template.format(param_name=param_name, func_name=c_func.name, cpp_type=cpp_type))
                elif cpp_type == "timestamp_tz_t":
                    input_line = f"\n\t\t\ttimestamp_tz_t meos_ts = DuckDBToMeosTimestamp(args{idx});\n"
                    input_lines.append(input_line)
                elif cpp_type == "MeosInterval *":
                    input_line = f"\n\t\t\tMeosInterval *{param_name} = IntervaltToInterval(args{idx});\n"
                    input_lines.append(input_line)
                elif cpp_type in PG_TYPE_MAP.values():
                    if c_func.name.endswith("_in") and cpp_type == "string_t":
                        input_line = f"\n\t\t\tstd::string {param_name}_0(args{idx}.GetDataUnsafe(), args{idx}.GetSize());\n"
                        input_line += f"\n\t\t\tconst char *{param_name} = {param_name}_0.c_str();\n"
                    else:
                        input_line = f"\n\t\t\t{cpp_type} {param_name} = args{idx};\n"
                    input_lines.append(input_line)
                else:
                    print(f"Unsupported parameter type: {param_type}")
                    raise ValueError(f"Unsupported parameter type: {param_type}")

        implementation += "\t\t".join(input_lines)

        # TODO: processing
        # basetype
        if c_func.needs_basetype:
            for i in range(len(sql_func.parameters)):
                if c_func.parameters[i][0] == "DATUM":
                    basetype = BASETYPE_MAP[sql_func.parameters[i][0]]
                    implementation += f"\t\t\tmeosType basetype = DataHelpers::getMeosType(args{i});\n"
                    break

        # Parse processing line(s)
        processing_lines = []
        impl_lines = [line.strip() for line in c_func.implementation.split("\n") if line != ""]
        for line in impl_lines:
            if "PG_GETARG" in line or "meosType basetype" in line:
                continue
            else:
                processing_lines.append(line)

        processing_code = []
        output_type = PG_TYPE_MAP[c_func.meos_return_type]
        # print(processing_lines)
        if len(processing_lines) == 1:
            """
            Case: 1 line:
            PG_RETURN_*(meos_func(meos_args))
            TODO: check meos_args for reserved keywords
            """
            meos_args = self._validate_meos_args(c_func.meos_args, c_func.parameters)
            line = f"\t\t\t{output_type} ret = {c_func.meos_func}({', '.join(meos_args)});"
            processing_code.append(line)
            # print("Case 1")
            # print(processing_code)
        elif len(processing_lines) == 2:
            """
            Case: 2 lines:
            ['TBox *result = span_tbox(s);', 'PG_RETURN_TBOX_P(result);']
            meos_func = None, meos_args = [result]
            Need to parse line[-2]
            """
            meos_func, meos_args_og = self._parse_meos_case_2(processing_lines)
            meos_args = self._validate_meos_args(meos_args_og, c_func.parameters)
            line = f"\t\t\t{output_type} ret = {meos_func}({', '.join(meos_args)});"
            processing_code.append(line)
            # print("Case 2")
            # print(processing_code)
        elif len(processing_lines) == 3:
            """
            Case: 3 lines:
            ['TBox *result = palloc(sizeof(TBox));', 'spanset_tbox_slice(d, result);', 'PG_RETURN_TBOX_P(result);']
            meos_func = None, meos_args = [result]
            Has a palloc()
            """
            meos_func, meos_args_og = self._parse_meos_case_3(processing_lines)
            meos_args = self._validate_meos_args(meos_args_og, c_func.parameters)
            processing_code.append(f"\t\t\t{output_type}ret = ({output_type})malloc(sizeof({output_type[:-2]}));")
            processing_code.append(f"\n\t\t\t{meos_func}({', '.join(meos_args)});")
            # print("Case 3")
            # print(processing_code)
        elif len(processing_lines) == 4 and c_func.nullable:
            """
            Case: 4 lines:
            - Subcase: ['double result;', 'if (! tbox_xmin(box, &result))', 'PG_RETURN_NULL();', 'PG_RETURN_FLOAT8(result);']
            - Subcase: ['TBox *result = tbox_expand_value(box, value, basetype);', 'if (! result)', 'PG_RETURN_NULL();', 'PG_RETURN_TBOX_P(result);']
            meos_func = None, meos_args = [result]
            Has a null check, nullable=True
            """
            var_type, var_name, meos_func, meos_args_og = self._parse_meos_case_4(processing_lines)
            meos_args = self._validate_meos_args(meos_args_og, c_func.parameters)
            # print(var_type, var_name, meos_func, meos_args)
            if var_type is not None:
                # Subcase 1
                if var_type == "TimestampTz":
                    var_name = "ret_meos"
                    for i in range(len(meos_args)):
                        if meos_args[i] == "&ret":
                            meos_args[i] = "&ret_meos"
                processing_code.append(f"\t\t\t{var_type} {var_name};")
                processing_code.append(f"\n\t\t\tif (!{meos_func}({', '.join(meos_args)})) {{")
                processing_code.append(f"\n\t\t\t\tfree({meos_args[0]});")
                processing_code.append("\n\t\t\t\tmask.SetInvalid(idx);")
                output_type_str = "string_t" if output_type[:-2].lower() in MOBILITYDUCK_CUSTOM_TYPES else output_type
                processing_code.append(f"\n\t\t\t\treturn {output_type_str}();")
                processing_code.append("\n\t\t\t}")
                if var_type == "TimestampTz":
                    processing_code.append("\n\t\t\ttimestamp_tz_t ret = MeosToDuckDBTimestamp((timestamp_tz_t)ret_meos);")
            else:
                # Subcase 2
                processing_code.append(f"\t\t\t{output_type}ret = {meos_func}({', '.join(meos_args)});")
                processing_code.append("\n\t\t\tif (!ret) {")
                processing_code.append(f"\n\t\t\t\tfree({meos_args[0]});")
                processing_code.append("\n\t\t\t\tmask.SetInvalid(idx);")
                output_type_str = "string_t" if output_type[:-2].lower() in MOBILITYDUCK_CUSTOM_TYPES else output_type
                processing_code.append(f"\n\t\t\t\treturn {output_type_str}();")
                processing_code.append("\n\t\t\t}")
            # print(processing_code)
            # print("Case 4")
        elif len(processing_lines) == 4 and not c_func.nullable:
            processing_code = self._parse_meos_case_4_not_nullable(processing_lines)
            # print(processing_code)
            # print("Case 4 not nullable")
        else:
            """
            Case: special case:
            ["/* Can't do anything with null inputs */", 'if (! box1 && ! box2)', 'PG_RETURN_NULL();', 'TBox *result = palloc(sizeof(TBox));', '/* One of the boxes is null, return the other one */', 'if (! box1)', '{', 'memcpy(result, box2, sizeof(TBox));', 'PG_RETURN_TBOX_P(result);', '}', 'if (! box2)', '{', 'memcpy(result, box1, sizeof(TBox));', 'PG_RETURN_TBOX_P(result);', '}', '/* Both boxes are not null */', 'memcpy(result, box1, sizeof(TBox));', 'tbox_expand(box2, result);', 'PG_RETURN_TBOX_P(result);']
            """
            processing_code.append("\n\t\t\t// TODO: handle this case");
            # print("Case else")
        # free(...);
        # return ret;
        implementation += "\t\t".join(processing_code)
        output_lines = []
        if "*" in output_type and sql_func.return_type in MOBILITYDUCK_CUSTOM_TYPES:
            # Need to return string_t
            output_template = """
            string_t blob = StringVector::AddStringOrBlob(result, (const char *)ret, VARSIZE(ret));
"""
            output_lines.append(output_template)
        else:
            output_template = """
"""
            output_lines.append(output_template)
        for (param_type, param_name, idx, cast) in c_func.parameters:
            if "_P" in param_type:
                output_lines.append(f"\n\t\t\tfree({param_name});")

        if (output_type[:-2].lower() in MOBILITYDUCK_CUSTOM_TYPES):
            output_lines.append(f"\n\t\t\tfree(ret);")

        implementation += "\t\t".join(output_lines)
        implementation += "\n\t\t"
        if "*" in output_type and sql_func.return_type in MOBILITYDUCK_CUSTOM_TYPES:
            implementation += f"\treturn blob;\n\t\t}}\n\t);"
        else:
            implementation += f"\treturn ret;\n\t\t}}\n\t);"

        return implementation

class CodeGenerator:
    def __init__(self, output_dir: str = "generated",
                 mobilitydb_sql_dir: str = os.getenv("MOBILITYDB_SQL_DIR"),
                 mobilitydb_src_dir: str = os.getenv("MOBILITYDB_SRC_DIR"),
                 struct_name: str = "TemporaryStruct"):
        self.output_dir = Path(output_dir)
        self.mobilitydb_sql_dir = mobilitydb_sql_dir
        self.output_dir.mkdir(exist_ok=True)
        self.struct_name = struct_name
        self.sql_parser = MobilityDBSQLParser(mobilitydb_sql_dir)
        self.c_parser = MobilityDBCParser(mobilitydb_src_dir)
        self.sql_to_c_mapping = dict()
        self.template_generator = DuckDBTemplateGenerator(struct_name=struct_name)
        
    def parse_sql_functions(self, sql_files: List[str] = None):
        print("Starting SQL function parsing...")
        if sql_files:
            sql_functions = []
            for sql_file in sql_files:
                functions = self.sql_parser.parse_sql_file(sql_file)
                sql_functions.extend(functions)
                print(f"  Parsed {sql_file}: {len(functions)} functions")

            # for func in sql_functions:
            #     print(func)
            #     print("-" * 80)
        
            return sql_functions

    def parse_c_functions(self, c_files: List[str] = None):
        print("Starting C function parsing...")
        if c_files:
            c_functions = []
            for c_file in c_files:
                functions = self.c_parser.parse_c_file(c_file)
                c_functions.extend(functions)
                print(f"  Parsed {c_file}: {len(functions)} functions")

            # for func in c_functions:
            #     print(func)
            #     print("-" * 80)
        
        return c_functions

    def match_sql_and_c_functions(self, sql_functions: List[SQLFunction], c_functions: List[CFunction]):
        mapping = dict()
        for sql_func in sql_functions:
            for c_func in c_functions:
                if sql_func.c_function == c_func.name:
                    # print(f"Matched {sql_func.hash_str} to {c_func.name}")
                    mapping[sql_func.hash_str] = c_func
                    break
        return mapping

    def disambiguate_functions(self, sql_functions: List[SQLFunction], c_functions: List[CFunction]):
        mapping = dict()
        for c_func in c_functions:
            for sql_func in sql_functions:
                if sql_func.c_function == c_func.name:
                    if c_func.name not in mapping:
                        mapping[c_func.name] = []
                    mapping[c_func.name].append(sql_func)
        disamg = dict()
        for name, sql_funcs in mapping.items():
            if name not in disamg:
                disamg[name] = []
            disamg_keys = []
            if len(sql_funcs) == 1:
                # Not overloaded
                disamg_keys.append(name)
                if sql_funcs[0].is_cast:
                    # Also a cast function
                    disamg_keys.append(f"{name}_cast")
            else:
                # Overloaded
                for sql_func in sql_funcs:
                    disamg_suffix = '_'.join([param[0] for param in sql_func.parameters])
                    disamg_keys.append(f"{name}_{disamg_suffix}")
                    if sql_func.is_cast:
                        # Also a cast function
                        disamg_keys.append(f"{name}_{disamg_suffix}_cast")
            for disamg_key in disamg_keys:
                disamg[name].append(disamg_key)
        
        return disamg

    def generate_duckdb_templates(self, sql_functions: List[SQLFunction], c_functions: List[CFunction], disamg_mapping: Dict[str, List[str]]) -> List[DuckDBTemplate]:
        templates = []
        implemented_functions = set()
        for sql_func in sql_functions:
            for c_func in c_functions:
                if sql_func.c_function == c_func.name:
                    if c_func.name in disamg_mapping:
                        funcs = disamg_mapping[c_func.name]
                        for disamg_key in funcs:
                            if disamg_key not in implemented_functions:
                                try:
                                    implemented_functions.add(disamg_key)
                                    print(f"SQL: {sql_func.name} --> C: {c_func.name} --> {disamg_key}")
                                    templs = self.template_generator.generate_function(sql_func, c_func, disamg_key)
                                    templates.extend(templs)
                                except Exception as e:
                                    print(f"Error generating template for {sql_func.name}: {e}")
                                    print("-" * 80)
                                    continue
                                print("-" * 80)
        return templates

    def generate_duckdb_cpp_header(self, class_name: str):
        # Hardcoded name for now
        header_file = f"gen/{class_name}_functions.hpp"
        header_template = """#include "meos_wrapper_simple.hpp"
#include "common.hpp"
#include "{header_file}"
#include "time_util.hpp"
#include "type_util.hpp"
#include "tydef.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {{

"""
        return header_template.format(header_file=header_file)

    def generate_duckdb_footer(self):
        return """} // namespace duckdb"""

    def generate_duckdb_cpp(self, class_name: str, templates: List[DuckDBTemplate]):
        cpp_code = ""
        cpp_code += self.generate_duckdb_cpp_header(class_name)
        for template in templates:
            cpp_code += template.cpp_code + "\n"
        cpp_code += self.generate_duckdb_footer()
        
        filename = f"{class_name}_functions.cpp"
        with open(self.output_dir / filename, "w") as f:
            f.write(cpp_code)

    def generate_duckdb_hpp_header(self):
        return """#pragma once

#include "meos_wrapper_simple.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {

class ExtensionLoader;

"""

    def generate_duckdb_hpp_footer(self):
        return """\n} // namespace duckdb"""

    def generate_duckdb_header(self, class_name: str, struct_name: str, templates: List[DuckDBTemplate]):
        header_code = self.generate_duckdb_hpp_header()
        header_code += f"struct {struct_name} {{\n"
        for template in templates:
            headline = template.cpp_code.split("\n")[0]
            headline = headline.replace(f"{struct_name}::", "").replace(" {", "")
            header_code += f"\tstatic {headline};\n"
        header_code += "};\n"
        header_code += self.generate_duckdb_hpp_footer()

        filename = f"{class_name}_functions.hpp"
        with open(self.output_dir / filename, "w") as f:
            f.write(header_code)

    def move_to_gen_dir(self):
        for file in self.output_dir.glob("*.hpp"):
            shutil.move(file, "../src/include/gen/" + file.name)
        for file in self.output_dir.glob("*.cpp"):
            shutil.move(file, "../src/gen/" + file.name)

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Generate DuckDB extension from MobilityDB")
    parser.add_argument("--output-dir", default="generated", help="Output directory")
    parser.add_argument("--sql-files", nargs="*", help="Specific SQL files to parse")
    parser.add_argument("--c-files", nargs="*", help="Specific C files to parse")

    args = parser.parse_args()

    struct_name = "TboxFunctions"
    generator = CodeGenerator(output_dir=args.output_dir, struct_name=struct_name)
    sql_functions = generator.parse_sql_functions(args.sql_files)
    c_functions = generator.parse_c_functions(args.c_files)
    generator.sql_to_c_mapping = generator.match_sql_and_c_functions(sql_functions, c_functions)
    disamg_mapping = generator.disambiguate_functions(sql_functions, c_functions)
    templates = generator.generate_duckdb_templates(sql_functions, c_functions, disamg_mapping)
    print(f"Generated {len(templates)} templates")
    generator.generate_duckdb_header("tbox_temp", struct_name, templates)
    generator.generate_duckdb_cpp("tbox_temp", templates)
    generator.move_to_gen_dir()

if __name__ == "__main__":
    main()
    