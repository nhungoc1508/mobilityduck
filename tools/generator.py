import os
import sys
import re
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from dotenv import load_dotenv
load_dotenv("generator.env")

CPP_RETURN_TYPE_MAP = {
    "boolean": "bool",
    "integer": "int64_t",
    "timestamptz": "timestamp_tz_t",
    "float": "double"
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
    'tgeometry', 'tgeompoint'
]

PG_TYPE_MAP = {
    "TBOX_P": "TBox *"
}

RESERVED_KWS_MAP = {
    "result": "ret"
}

@dataclass
class SQLFunction:
    """Represents a MobilityDB SQL function"""
    name: str
    return_type: str
    parameters: List[Tuple[str, str]]  # (type, name) pairs
    c_function: str
    sql_definition: str
    category: str = "unknown"
    is_operator: bool = False
    operator_symbol: Optional[str] = None
    is_aggregate: bool = False
    is_cast: bool = False

    def __str__(self):
        return f"SQLFunction(name={self.name}\n\treturn_type={self.return_type}\n\tparameters={self.parameters}\n\tc_function={self.c_function}\n\tcategory={self.category}\n\tis_operator={self.is_operator}\n\tis_aggregate={self.is_aggregate}\n\tis_cast={self.is_cast})"

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
            category = self._determine_category(func_name, parameters, return_type, func_line_number)
            is_operator = self._is_operator_function(category)
            is_aggregate = self._is_aggregate_function(category)
            is_cast = self._is_cast_function(func_name, parameters)

            sql_func = SQLFunction(
                name=func_name,
                return_type=return_type,
                parameters=parameters,
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
                if len(parts) >= 1:
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
        return False
    
    # def parse_all_sql_files(self) -> List[SQLFunction]:

@dataclass
class CFunction:
    name: str
    parameters: List[Tuple[str, str]]
    needs_basetype: bool
    meos_args: List[str]
    meos_return_type: str
    nullable: bool
    implementation: str

    def __str__(self):
        return f"CFunction(name={self.name}\n\tparameters={self.parameters}\n\tneeds_basetype={self.needs_basetype}\n\tmeos_args={self.meos_args}\n\tmeos_return_type={self.meos_return_type}\n\tnullable={self.nullable}\n\timplementation={self.implementation})"

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
                meos_args = self._extract_meos_args(implementation, func_name)
                meos_return_type, nullable = self._extract_meos_return_type(implementation)

            c_func = CFunction(
                name=func_name,
                parameters=parameters,
                needs_basetype=needs_basetype,
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

    def _extract_meos_args(self, implementation: str, func_name: str) -> List[str]:
        # Match the function call inside PG_RETURN_* and capture args
        func_name = func_name.lower()
        pattern = re.compile(r'\b' + re.escape(func_name) + r'\s*\((.*)\)(?=\s*;)')
        match = re.search(pattern, implementation)
        args = []
        if match:
            args_str = match.group(1)
            # Split by commas and strip
            args = [arg.strip() for arg in args_str.split(",")]
        return args

@dataclass
class DuckDBTemplate:
    name: str
    template_type: str
    cpp_code: str

class DuckDBTemplateGenerator:
    def __init__(self):
        self.templates = self._build_templates()

    def _build_templates(self) -> Dict[str, str]:
        return {
            'scalar_func': """
void {struct_name}::{function_name}(DataChunk &args, ExpressionState &state, Vector &result) {{
    {implementation}
    if (args.size() == 1) {{
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }}
}}
""",
            'cast_func': """
bool {struct_name}::{function_name}(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {{
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

    def generate_function(self, sql_func: SQLFunction, c_func: CFunction) -> DuckDBTemplate:
        # If in/out -- only cast
        if sql_func.category != 'input/output':
            template = self.templates['scalar_func']
            return_type = sql_func.return_type if sql_func.return_type in MOBILITYDUCK_CUSTOM_TYPES else CPP_RETURN_TYPE_MAP[sql_func.return_type]
            struct_name = self._get_struct_name(sql_func.name)
            implementation = self._generate_implementation(sql_func, c_func)
            print(f"Struct name: {struct_name}")
            print(f"Function name: {sql_func.c_function}")
            print(f"Implementation:\n{implementation}")
            cpp_code = template.format(
                struct_name=struct_name,
                function_name=sql_func.c_function,
                implementation=implementation
            )

            return DuckDBTemplate(
                name=sql_func.name,
                template_type="scalar_func",
                cpp_code=cpp_code
            )
        else:
            return None

    def _get_struct_name(self, function_name: str) -> str:
        return "TemporaryStruct"

    def _generate_implementation(self, sql_func: SQLFunction, c_func: CFunction) -> str:
        print(sql_func)
        print(c_func)
        implementation = ""
        # UnaryExecutor::Execute<string_t, bool>(
        num_args = len(sql_func.parameters)
        executor = EXECUTOR_MAP[num_args]
        implementation += f"\t{executor}::Execute{'WithNulls' if c_func.nullable else ''}"
        executor_params = []
        for param in sql_func.parameters:
            if param[0] in MOBILITYDUCK_CUSTOM_TYPES:
                executor_params.append("string_t")
            else:
                executor_params.append(CPP_RETURN_TYPE_MAP[param[0]])
        implementation += f"<{', '.join(executor_params)}"
        implementation += f", {'string_t' if sql_func.return_type in MOBILITYDUCK_CUSTOM_TYPES else CPP_RETURN_TYPE_MAP[sql_func.return_type]}>(\n"
        # args.data[0], result, args.size(),
        arg_strs = []
        for i in range(num_args):
            arg_strs.append(f"args.data[{i}]")
        implementation += f"\t\t"
        implementation += f", ".join(arg_strs)
        implementation += f", "
        implementation += f"result, args.size(),\n\t\t[&]("
        # [&](string_t tbox_str) {
        param_strs = []
        for i, param_type in enumerate(executor_params):
            param_strs.append(f"{param_type} args{i}")
        implementation += f", ".join(param_strs)
        implementation += ") {"

        # TBox *tbox = ...
        input_lines = []
        for (param_type, param_name, idx, cast) in c_func.parameters:
            cpp_type = PG_TYPE_MAP[param_type]
            if param_name in RESERVED_KWS_MAP:
                param_name = RESERVED_KWS_MAP[param_name]
            input_template = """
            {cpp_type}{param_name} = nullptr;
            if (args{idx}.GetSize() > 0) {{
                {param_name} = ({cpp_type})malloc(args{idx}.GetSize());
                memcpy({param_name}, args{idx}.GetDataUnsafe(), args{idx}.GetSize());
            }}
            if (!{param_name}) {{
                throw InternalException("Failure in {func_name}: unable to cast binary to {cpp_type}");
            }}
"""
            input_lines.append(input_template.format(cpp_type=cpp_type, param_name=param_name, idx=idx, func_name=c_func.name))
        implementation += "\n\t\t".join(input_lines)

        return implementation

class CodeGenerator:
    def __init__(self, output_dir: str = "generated",
                 mobilitydb_sql_dir: str = os.getenv("MOBILITYDB_SQL_DIR"),
                 mobilitydb_src_dir: str = os.getenv("MOBILITYDB_SRC_DIR")):
        self.output_dir = Path(output_dir)
        self.mobilitydb_sql_dir = mobilitydb_sql_dir
        self.output_dir.mkdir(exist_ok=True)

        self.sql_parser = MobilityDBSQLParser(mobilitydb_sql_dir)
        self.c_parser = MobilityDBCParser(mobilitydb_src_dir)
        self.sql_to_c_mapping = dict()
        self.template_generator = DuckDBTemplateGenerator()
        
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
                    mapping[sql_func.name] = c_func
                    break
        return mapping

    def generate_duckdb_templates(self, sql_functions: List[SQLFunction], sql_to_c_mapping: Dict[str, CFunction]) -> List[DuckDBTemplate]:
        templates = []
        for sql_func in sql_functions:
            if sql_func.name in sql_to_c_mapping:
                c_func = sql_to_c_mapping[sql_func.name]
                try:
                    template = self.template_generator.generate_function(sql_func, c_func)
                    templates.append(template)
                    print(f"Generated template for {sql_func.name}")
                except Exception as e:
                    print(f"Error generating template for {sql_func.name}: {e}")
                    print("-" * 80)
                print("-" * 80)
        return templates

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Generate DuckDB extension from MobilityDB")
    parser.add_argument("--output-dir", default="generated", help="Output directory")
    parser.add_argument("--sql-files", nargs="*", help="Specific SQL files to parse")
    parser.add_argument("--c-files", nargs="*", help="Specific C files to parse")

    args = parser.parse_args()

    generator = CodeGenerator(output_dir=args.output_dir)
    sql_functions = generator.parse_sql_functions(args.sql_files)
    c_functions = generator.parse_c_functions(args.c_files)
    generator.sql_to_c_mapping = generator.match_sql_and_c_functions(sql_functions, c_functions)
    templates = generator.generate_duckdb_templates(sql_functions, generator.sql_to_c_mapping)

if __name__ == "__main__":
    main()
    