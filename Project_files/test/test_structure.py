import base64
import unittest

import app.dataframe_builder


class StructureTestSuite(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.MODULE = app.dataframe_builder
    
    def test_class_exists_dataframebuilder(self):
        classes = _get_class_names(self.MODULE)
        self.assertIn(
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            classes,
            msg=f"The class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"is not found, but it was marked as required."
        )
        
    def test_class_function_exists_dataframebuilder___init__(self):
        classes = _get_class_names(self.MODULE)
        self.assertIn(
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            classes,
            msg=f"The class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"is not found, but it was marked as required."
        )
        functions = _get_class_function_names(
            self.MODULE,
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()
        )
        self.assertIn(
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5fX2luaXRfXw==').decode(),
            functions,
            msg=f"The function "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5fX2luaXRfXw==').decode()}` "
                f"is not found in class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}`, "
                f"but it was marked as required."
        )
        
    def test_class_function_signature_match_dataframebuilder___init__(self):
        classes = _get_class_names(self.MODULE)
        self.assertIn(
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            classes,
            msg=f"The class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"is not found, but it was marked as required."
        )
        functions = _get_class_function_names(
            self.MODULE,
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()
        )
        self.assertIn(
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5fX2luaXRfXw==').decode(),
            functions,
            msg=f"The function "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5fX2luaXRfXw==').decode()}` "
                f"is not found in class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}`, "
                f"but it was marked as required."
        )
        args = _get_class_function_arg_names(
            self.MODULE,
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5fX2luaXRfXw==').decode()
        )
        self.assertEqual(
            2,
            len(args),
            msg=f"The function "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5fX2luaXRfXw==').decode()}` "
                f"in class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"should have 2 argument(s)."
        )
        args = _get_class_function_arg_names(
            self.MODULE,
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5fX2luaXRfXw==').decode()
        )
        self.assertEqual(
            base64.b64decode(b'c2VsZg==').decode(),
            args[0],
            msg=f"The argument #0 of function "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5fX2luaXRfXw==').decode()}` "
                f"in class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"should be called "
                f"`self`."
        )
        args = _get_class_function_arg_names(
            self.MODULE,
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5fX2luaXRfXw==').decode()
        )
        self.assertEqual(
            base64.b64decode(b'ZGY=').decode(),
            args[1],
            msg=f"The argument #1 of function "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5fX2luaXRfXw==').decode()}` "
                f"in class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"should be called "
                f"`df`."
        )
        
    def test_class_function_exists_dataframebuilder_create_df_from_csv(self):
        classes = _get_class_names(self.MODULE)
        self.assertIn(
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            classes,
            msg=f"The class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"is not found, but it was marked as required."
        )
        functions = _get_class_function_names(
            self.MODULE,
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()
        )
        self.assertIn(
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5jcmVhdGVfZGZfZnJvbV9jc3Y=').decode(),
            functions,
            msg=f"The function "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5jcmVhdGVfZGZfZnJvbV9jc3Y=').decode()}` "
                f"is not found in class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}`, "
                f"but it was marked as required."
        )
        
    def test_class_function_signature_match_dataframebuilder_create_df_from_csv(self):
        classes = _get_class_names(self.MODULE)
        self.assertIn(
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            classes,
            msg=f"The class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"is not found, but it was marked as required."
        )
        functions = _get_class_function_names(
            self.MODULE,
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()
        )
        self.assertIn(
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5jcmVhdGVfZGZfZnJvbV9jc3Y=').decode(),
            functions,
            msg=f"The function "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5jcmVhdGVfZGZfZnJvbV9jc3Y=').decode()}` "
                f"is not found in class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}`, "
                f"but it was marked as required."
        )
        args = _get_class_function_arg_names(
            self.MODULE,
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5jcmVhdGVfZGZfZnJvbV9jc3Y=').decode()
        )
        self.assertEqual(
            2,
            len(args),
            msg=f"The function "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5jcmVhdGVfZGZfZnJvbV9jc3Y=').decode()}` "
                f"in class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"should have 2 argument(s)."
        )
        args = _get_class_function_arg_names(
            self.MODULE,
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5jcmVhdGVfZGZfZnJvbV9jc3Y=').decode()
        )
        self.assertEqual(
            base64.b64decode(b'c2VsZg==').decode(),
            args[0],
            msg=f"The argument #0 of function "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5jcmVhdGVfZGZfZnJvbV9jc3Y=').decode()}` "
                f"in class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"should be called "
                f"`self`."
        )
        args = _get_class_function_arg_names(
            self.MODULE,
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5jcmVhdGVfZGZfZnJvbV9jc3Y=').decode()
        )
        self.assertEqual(
            base64.b64decode(b'cGF0aA==').decode(),
            args[1],
            msg=f"The argument #1 of function "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5jcmVhdGVfZGZfZnJvbV9jc3Y=').decode()}` "
                f"in class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"should be called "
                f"`path`."
        )
        
    def test_class_function_exists_dataframebuilder_create_spark_session(self):
        classes = _get_class_names(self.MODULE)
        self.assertIn(
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            classes,
            msg=f"The class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"is not found, but it was marked as required."
        )
        functions = _get_class_function_names(
            self.MODULE,
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()
        )
        self.assertIn(
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5jcmVhdGVfc3Bhcmtfc2Vzc2lvbg==').decode(),
            functions,
            msg=f"The function "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5jcmVhdGVfc3Bhcmtfc2Vzc2lvbg==').decode()}` "
                f"is not found in class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}`, "
                f"but it was marked as required."
        )
        
    def test_class_function_signature_match_dataframebuilder_create_spark_session(self):
        classes = _get_class_names(self.MODULE)
        self.assertIn(
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            classes,
            msg=f"The class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"is not found, but it was marked as required."
        )
        functions = _get_class_function_names(
            self.MODULE,
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()
        )
        self.assertIn(
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5jcmVhdGVfc3Bhcmtfc2Vzc2lvbg==').decode(),
            functions,
            msg=f"The function "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5jcmVhdGVfc3Bhcmtfc2Vzc2lvbg==').decode()}` "
                f"is not found in class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}`, "
                f"but it was marked as required."
        )
        args = _get_class_function_arg_names(
            self.MODULE,
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5jcmVhdGVfc3Bhcmtfc2Vzc2lvbg==').decode()
        )
        self.assertEqual(
            1,
            len(args),
            msg=f"The function "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5jcmVhdGVfc3Bhcmtfc2Vzc2lvbg==').decode()}` "
                f"in class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"should have 1 argument(s)."
        )
        args = _get_class_function_arg_names(
            self.MODULE,
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5jcmVhdGVfc3Bhcmtfc2Vzc2lvbg==').decode()
        )
        self.assertEqual(
            base64.b64decode(b'c2VsZg==').decode(),
            args[0],
            msg=f"The argument #0 of function "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5jcmVhdGVfc3Bhcmtfc2Vzc2lvbg==').decode()}` "
                f"in class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"should be called "
                f"`self`."
        )
        
    def test_class_function_exists_dataframebuilder_customer_purchases_by_country(self):
        classes = _get_class_names(self.MODULE)
        self.assertIn(
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            classes,
            msg=f"The class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"is not found, but it was marked as required."
        )
        functions = _get_class_function_names(
            self.MODULE,
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()
        )
        self.assertIn(
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5jdXN0b21lcl9wdXJjaGFzZXNfYnlfY291bnRyeQ==').decode(),
            functions,
            msg=f"The function "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5jdXN0b21lcl9wdXJjaGFzZXNfYnlfY291bnRyeQ==').decode()}` "
                f"is not found in class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}`, "
                f"but it was marked as required."
        )
        
    def test_class_function_signature_match_dataframebuilder_customer_purchases_by_country(self):
        classes = _get_class_names(self.MODULE)
        self.assertIn(
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            classes,
            msg=f"The class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"is not found, but it was marked as required."
        )
        functions = _get_class_function_names(
            self.MODULE,
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()
        )
        self.assertIn(
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5jdXN0b21lcl9wdXJjaGFzZXNfYnlfY291bnRyeQ==').decode(),
            functions,
            msg=f"The function "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5jdXN0b21lcl9wdXJjaGFzZXNfYnlfY291bnRyeQ==').decode()}` "
                f"is not found in class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}`, "
                f"but it was marked as required."
        )
        args = _get_class_function_arg_names(
            self.MODULE,
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5jdXN0b21lcl9wdXJjaGFzZXNfYnlfY291bnRyeQ==').decode()
        )
        self.assertEqual(
            3,
            len(args),
            msg=f"The function "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5jdXN0b21lcl9wdXJjaGFzZXNfYnlfY291bnRyeQ==').decode()}` "
                f"in class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"should have 3 argument(s)."
        )
        args = _get_class_function_arg_names(
            self.MODULE,
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5jdXN0b21lcl9wdXJjaGFzZXNfYnlfY291bnRyeQ==').decode()
        )
        self.assertEqual(
            base64.b64decode(b'c2VsZg==').decode(),
            args[0],
            msg=f"The argument #0 of function "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5jdXN0b21lcl9wdXJjaGFzZXNfYnlfY291bnRyeQ==').decode()}` "
                f"in class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"should be called "
                f"`self`."
        )
        args = _get_class_function_arg_names(
            self.MODULE,
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5jdXN0b21lcl9wdXJjaGFzZXNfYnlfY291bnRyeQ==').decode()
        )
        self.assertEqual(
            base64.b64decode(b'Y3VzdG9tZXJfaWRfY29s').decode(),
            args[1],
            msg=f"The argument #1 of function "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5jdXN0b21lcl9wdXJjaGFzZXNfYnlfY291bnRyeQ==').decode()}` "
                f"in class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"should be called "
                f"`customer_id_col`."
        )
        args = _get_class_function_arg_names(
            self.MODULE,
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5jdXN0b21lcl9wdXJjaGFzZXNfYnlfY291bnRyeQ==').decode()
        )
        self.assertEqual(
            base64.b64decode(b'Y291bnRyeV9jb2w=').decode(),
            args[2],
            msg=f"The argument #2 of function "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5jdXN0b21lcl9wdXJjaGFzZXNfYnlfY291bnRyeQ==').decode()}` "
                f"in class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"should be called "
                f"`country_col`."
        )
        
    def test_class_function_exists_dataframebuilder_get_top_n_products_by_country(self):
        classes = _get_class_names(self.MODULE)
        self.assertIn(
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            classes,
            msg=f"The class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"is not found, but it was marked as required."
        )
        functions = _get_class_function_names(
            self.MODULE,
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()
        )
        self.assertIn(
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5nZXRfdG9wX25fcHJvZHVjdHNfYnlfY291bnRyeQ==').decode(),
            functions,
            msg=f"The function "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5nZXRfdG9wX25fcHJvZHVjdHNfYnlfY291bnRyeQ==').decode()}` "
                f"is not found in class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}`, "
                f"but it was marked as required."
        )
        
    def test_class_function_signature_match_dataframebuilder_get_top_n_products_by_country(self):
        classes = _get_class_names(self.MODULE)
        self.assertIn(
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            classes,
            msg=f"The class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"is not found, but it was marked as required."
        )
        functions = _get_class_function_names(
            self.MODULE,
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()
        )
        self.assertIn(
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5nZXRfdG9wX25fcHJvZHVjdHNfYnlfY291bnRyeQ==').decode(),
            functions,
            msg=f"The function "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5nZXRfdG9wX25fcHJvZHVjdHNfYnlfY291bnRyeQ==').decode()}` "
                f"is not found in class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}`, "
                f"but it was marked as required."
        )
        args = _get_class_function_arg_names(
            self.MODULE,
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5nZXRfdG9wX25fcHJvZHVjdHNfYnlfY291bnRyeQ==').decode()
        )
        self.assertEqual(
            4,
            len(args),
            msg=f"The function "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5nZXRfdG9wX25fcHJvZHVjdHNfYnlfY291bnRyeQ==').decode()}` "
                f"in class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"should have 4 argument(s)."
        )
        args = _get_class_function_arg_names(
            self.MODULE,
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5nZXRfdG9wX25fcHJvZHVjdHNfYnlfY291bnRyeQ==').decode()
        )
        self.assertEqual(
            base64.b64decode(b'c2VsZg==').decode(),
            args[0],
            msg=f"The argument #0 of function "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5nZXRfdG9wX25fcHJvZHVjdHNfYnlfY291bnRyeQ==').decode()}` "
                f"in class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"should be called "
                f"`self`."
        )
        args = _get_class_function_arg_names(
            self.MODULE,
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5nZXRfdG9wX25fcHJvZHVjdHNfYnlfY291bnRyeQ==').decode()
        )
        self.assertEqual(
            base64.b64decode(b'bg==').decode(),
            args[1],
            msg=f"The argument #1 of function "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5nZXRfdG9wX25fcHJvZHVjdHNfYnlfY291bnRyeQ==').decode()}` "
                f"in class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"should be called "
                f"`n`."
        )
        args = _get_class_function_arg_names(
            self.MODULE,
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5nZXRfdG9wX25fcHJvZHVjdHNfYnlfY291bnRyeQ==').decode()
        )
        self.assertEqual(
            base64.b64decode(b'cHJvZHVjdF9jb2w=').decode(),
            args[2],
            msg=f"The argument #2 of function "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5nZXRfdG9wX25fcHJvZHVjdHNfYnlfY291bnRyeQ==').decode()}` "
                f"in class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"should be called "
                f"`product_col`."
        )
        args = _get_class_function_arg_names(
            self.MODULE,
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5nZXRfdG9wX25fcHJvZHVjdHNfYnlfY291bnRyeQ==').decode()
        )
        self.assertEqual(
            base64.b64decode(b'Y291bnRyeV9jb2w=').decode(),
            args[3],
            msg=f"The argument #3 of function "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5nZXRfdG9wX25fcHJvZHVjdHNfYnlfY291bnRyeQ==').decode()}` "
                f"in class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"should be called "
                f"`country_col`."
        )
        
    def test_class_function_exists_dataframebuilder_sample(self):
        classes = _get_class_names(self.MODULE)
        self.assertIn(
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            classes,
            msg=f"The class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"is not found, but it was marked as required."
        )
        functions = _get_class_function_names(
            self.MODULE,
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()
        )
        self.assertIn(
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5zYW1wbGU=').decode(),
            functions,
            msg=f"The function "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5zYW1wbGU=').decode()}` "
                f"is not found in class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}`, "
                f"but it was marked as required."
        )
        
    def test_class_function_signature_match_dataframebuilder_sample(self):
        classes = _get_class_names(self.MODULE)
        self.assertIn(
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            classes,
            msg=f"The class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"is not found, but it was marked as required."
        )
        functions = _get_class_function_names(
            self.MODULE,
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()
        )
        self.assertIn(
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5zYW1wbGU=').decode(),
            functions,
            msg=f"The function "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5zYW1wbGU=').decode()}` "
                f"is not found in class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}`, "
                f"but it was marked as required."
        )
        args = _get_class_function_arg_names(
            self.MODULE,
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5zYW1wbGU=').decode()
        )
        self.assertEqual(
            2,
            len(args),
            msg=f"The function "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5zYW1wbGU=').decode()}` "
                f"in class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"should have 2 argument(s)."
        )
        args = _get_class_function_arg_names(
            self.MODULE,
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5zYW1wbGU=').decode()
        )
        self.assertEqual(
            base64.b64decode(b'c2VsZg==').decode(),
            args[0],
            msg=f"The argument #0 of function "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5zYW1wbGU=').decode()}` "
                f"in class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"should be called "
                f"`self`."
        )
        args = _get_class_function_arg_names(
            self.MODULE,
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode(),
            base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5zYW1wbGU=').decode()
        )
        self.assertEqual(
            base64.b64decode(b'cmF0aW8=').decode(),
            args[1],
            msg=f"The argument #1 of function "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlci5zYW1wbGU=').decode()}` "
                f"in class "
                f"`{base64.b64decode(b'RGF0YWZyYW1lQnVpbGRlcg==').decode()}` "
                f"should be called "
                f"`ratio`."
        )
        

# === Internal functions, do not modify ===
import inspect

from types import ModuleType
from typing import List


def _get_function_names(module: ModuleType) -> List[str]:
    names = []
    functions = inspect.getmembers(module, lambda member: inspect.isfunction(member))
    for name, fn in functions:
        if fn.__module__ == module.__name__:
            names.append(name)
    return names


def _get_function_arg_names(module: ModuleType, fn_name: str) -> List[str]:
    arg_names = []
    functions = inspect.getmembers(module, lambda member: inspect.isfunction(member))
    for name, fn in functions:
        if fn.__module__ == module.__name__:
            if fn.__qualname__ == fn_name:
                args_spec = inspect.getfullargspec(fn)
                arg_names = args_spec.args
                if args_spec.varargs is not None:
                    arg_names.extend(args_spec.varargs)
                if args_spec.varkw is not None:
                    arg_names.extend(args_spec.varkw)
                arg_names.extend(args_spec.kwonlyargs)
                break
    return arg_names


def _get_class_names(module: ModuleType) -> List[str]:
    names = []
    classes = inspect.getmembers(module, lambda member: inspect.isclass(member))
    for name, cls in classes:
        if cls.__module__ == module.__name__:
            names.append(name)
    return names


def _get_class_function_names(module: ModuleType, cls_name: str) -> List[str]:
    fn_names = []
    classes = inspect.getmembers(module, lambda member: inspect.isclass(member))
    for cls_name_, cls in classes:
        if cls.__module__ == module.__name__:
            if cls_name_ == cls_name:
                functions = inspect.getmembers(
                    cls,
                    lambda member: inspect.ismethod(member)
                    or inspect.isfunction(member),
                )
                for fn_name, fn in functions:
                    fn_names.append(fn.__qualname__)
                break
    return fn_names


def _get_class_function_arg_names(
    module: ModuleType, cls_name: str, fn_name: str
) -> List[str]:
    arg_names = []
    classes = inspect.getmembers(module, lambda member: inspect.isclass(member))
    for cls_name_, cls in classes:
        if cls.__module__ == module.__name__:
            if cls_name_ == cls_name:
                functions = inspect.getmembers(
                    cls,
                    lambda member: inspect.ismethod(member)
                    or inspect.isfunction(member),
                )
                for fn_name_, fn in functions:
                    if fn.__qualname__ == fn_name:
                        args_spec = inspect.getfullargspec(fn)
                        arg_names = args_spec.args
                        if args_spec.varargs is not None:
                            arg_names.extend(args_spec.varargs)
                        if args_spec.varkw is not None:
                            arg_names.extend(args_spec.varkw)
                        arg_names.extend(args_spec.kwonlyargs)
                        break
                break
    return arg_names
