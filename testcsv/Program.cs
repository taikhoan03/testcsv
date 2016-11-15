using CsvHelper;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Dynamic;
using Newtonsoft.Json;
using System.Reflection;
using NCalc;
using Libs;
using DevUtils;
using DevUtils.PrimitivesExtensions;

namespace testcsv
{
    class Program
    {
        static void Main(string[] args)
        {
            var pro = new ReflectionClass();
            pro.call();

            using (TextReader reader = File.OpenText(@"D:\FA_in_out\InputFile\State 1\Bibb\CondoUnit2.txt"))
            {
                
                var d = DateTime.Now;
                //string line = reader.ReadLine();
                //Console.WriteLine(line);
                var config = new CsvHelper.Configuration.CsvConfiguration();
                config.BufferSize = 2048;
                config.Delimiter = "\t";
                config.IgnoreBlankLines = true;
                config.HasHeaderRecord = true;
                config.SkipEmptyRecords = true;
                
                var csv = new CsvReader(reader, config);
                Dictionary<string, object> dic = new Dictionary<string, object>();
                csv.ReadHeader();
                var fields = csv.FieldHeaders;

                while (csv.Read())
                {
                    var key = csv.GetField<string>(0);
                    if (!dic.ContainsKey(key))
                    {
                        dynamic MyDynamic = new System.Dynamic.ExpandoObject();
                        IDictionary<string, object> myUnderlyingObject = MyDynamic;
                        //MyDynamic.A = "A";
                        //MyDynamic.B = "B";
                        //MyDynamic.C = "C";
                        //MyDynamic.Number = 12;
                        //foreach (var field in fields)
                        //{
                        //    dic_fields.Add(field, csv.GetField<string>(field));
                        //}
                        for (var i = 0; i < fields.Length; i++)
                        {
                            
                            myUnderlyingObject.Add(fields[i], csv.GetField<string>(i));
                            
                            //MyDynamic.fields[i] = csv.GetField<string>(i);
                            //dic_fields.Add(fields[i], csv.GetField<string>(i));
                        }
                        dic.Add(key, MyDynamic);
                        
                    }
                    // else: key da ton tai
                    else
                    {

                    }
                    //   var intField = csv.GetField<string>(0);
                    //var stringField = csv.GetField<string>(1);
                    //var boolField = csv.GetField<string>("HeaderName");
                }
                var a = dic;

                //foreach (var item in dic)
                //{
                //    var pro = new ReflectionClass();
                //    pro.call();
                //}

                var array = dic.Select(p => p.Value).Take(50).ToArray();
                File.WriteAllText(@"D:\CondoUnit2.json", JsonConvert.SerializeObject(array));
                Console.WriteLine("time:" + (DateTime.Now - d).TotalSeconds);


                var columns_exp = new string[] { "{KEY_3}", "''+({Major}+3)+'_'+{Minor}" };//db column expression
                foreach (var item in columns_exp)
                {
                    foreach (var rec in array)
                    {
                        var b = item.FormatWith_ExpandObject(((ExpandoObject)rec));
                        var ex = new Expression(b);
                        ////ex.Parameters["variable"] = "Sample's Data";
                        var evaluated1 = ex.Evaluate();

                        Console.WriteLine(b);
                        Console.WriteLine(evaluated1.ToString());
                    }
                    
                }

                #region call function from string

                #endregion
                Console.ReadLine();
            }
        }
        

    }
    public class ReflectionClass
    {
        #region functions cmd
        public void call()
        {
            
            var expr = new Expression("yourfunctionname(3, '123')");
            expr.EvaluateFunction += NCalcExtensionFunctions;
            var result = expr.Evaluate();
            Console.WriteLine(result.ToString());

            var ee = new Expression("variable=='Sample\\'s Data'");
            ee.Parameters["variable"] = "Sample's Data";
            var evaluated = ee.Evaluate();
            Console.WriteLine(evaluated.ToString());

            var columns_exp = new string[] { "colA=1", "colB=colA+'abc'" };//db column expression
            foreach (var item in columns_exp)
            {
                var a = item.FormatWith("");
                //var ex = new Expression(item);
                ////ex.Parameters["variable"] = "Sample's Data";
                //var evaluated1 = ex.Evaluate();
                Console.WriteLine(a);
            }

            //Type thisType = this.GetType();
            //MethodInfo theMethod = thisType.GetMethod("sum");
            //var args = new string[] { "123\"", "456" };
            //theMethod.Invoke(this, new object[] { args });
            Expression e = new Expression("sum( 1,2,  3  ,'4')");
            var param = new Param()
            {
                ARITHMETICAL=new string[]
                {
                    "STRING_COUNT(Comm_txt.A, 8)",
                    "Comm_txt.A + Comm_txt.Major + 5"
                },
                CONDITIONAL = new string[]
                {
                    "CONTAINS(Rule-1, 2, 3, 4aa)",
                    "EQUAL(Comm_txt.A, 3)",
                },
                FORMATTING = new string[]
                {
                    "JOIN([0 Space(s)], Comm_txt.A, Comm_txt.Major)"
                },
                MISCELLANEOUS = new string[]
                {

                },
            };
            var student = new

            {

                Name = "John",

                Email = "john@roffle.edu",

                BirthDate = new DateTime(1983, 3, 20),

                Results = new[]

  {

    new { Name = "COMP101", Grade = 10 },

    new { Name = "ECON101", Grade = 9 }

  }

            };



            Console.WriteLine("Top result for {Name} was {Results[0].Name}".FormatWith(student));
            // tell it how to handle your custom function
            e.EvaluateFunction += delegate (string name, FunctionArgs args)
            {
                if (name == "sum")
                {
                    //args.Result = (int)args.Parameters[0].Evaluate() + (int)args.Parameters[1].Evaluate();
                    args.Result = sum(args.Parameters);
                }
                    
            };
            var Rule1 = e.Evaluate();



            #region dynamic variable naming
            Dictionary<string, int> names = new Dictionary<string, int>();


            for (int i = 0; i < 10; i++)
            {
                names.Add(String.Format("name{0}", i.ToString()), i);
            }

            var xx1 = names["name1"];
            var xx2 = names["name2"];
            var xx3 = names["name3"];
            #endregion dynamic variable naming
            dynamic MyDynamic = new System.Dynamic.ExpandoObject();
            
            IDictionary<string, object> myUnderlyingObject = MyDynamic;
            myUnderlyingObject["Rule1"] = Rule1;
            //string str = "'a'+{Rule1}".FormatWith(MyDynamic);
            Expression e2 = new Expression("'a'+[Rule1]");
            e2.EvaluateParameter += delegate (string name, ParameterArgs args)
            {
                if (name == "Rule1")
                    args.Result = Rule1;
            };
            var Rule2 = e2.Evaluate();


            
        }
        public int sum(object []args)
        {
            //if (args.Length == 0)
            //    return;
            Console.WriteLine("arg:" + args[0].ToString());
            return 111;
        }
        private static void NCalcExtensionFunctions(string name, FunctionArgs functionArgs)
        {
            if (name == "yourfunctionname")
            {
                var param1 = functionArgs.Parameters[0].Evaluate();
                var param2 = functionArgs.Parameters[1].Evaluate();
                //... as many params as you require
                functionArgs.Result =  param2;
                //functionArgs.Result = (int)param1 * (int)param2; //do your own function logic here
            }
            if (name == "random")
            {
                if (functionArgs.Parameters.Count() == 0)
                {
                    functionArgs.Result = new Random().Next();
                }
                else if (functionArgs.Parameters.Count() == 1)
                {
                    functionArgs.Result = new Random().Next((int)functionArgs.Parameters[0].Evaluate());
                }
                else
                {
                    functionArgs.Result = new Random().Next((int)functionArgs.Parameters[0].Evaluate(), (int)functionArgs.Parameters[1].Evaluate());
                }
            }
        }
        public string tong(double a, double b)
        {
            return "123";
        }
        #endregion functions cmd
    }
    /// <summary>
    /// User data
    /// </summary>
    public class UD
    {
        /// <summary>
        /// Input file Name
        /// </summary>
        public string Inp { get; set; }
        public List<Param> _Params { get; set; }
        public string OutputName { get; set; }

    }
    /// <summary>
    /// Params
    /// </summary>
    public class Param
    {
        /// <summary>
        /// Index
        /// </summary>
        public int Index { get; set; }
        /// <summary>
        /// User input content for calculate (User Input data)
        /// </summary>
        public string UserInputData { get; set; }
        /// <summary>
        /// Type
        /// </summary>
        public string Type { get; set; }


        //return string/int/value
        public string[] ARITHMETICAL { get; set; }
        //return true/false
        public string[] CONDITIONAL { get; set; }
        public string[] FORMATTING { get; set; }
        public string[] MISCELLANEOUS { get; set; }
    }
    //public class ARITHMETICAL
    //{

    //}
    //public class CONDITIONAL
    //{

    //}
    //public class FORMATTING
    //{

    //}
    //public class MISCELLANEOUS
    //{

    //}
    //public static string FormatWithExpandoObject(this string str)
    //{

    //    return "";
    //}
}
public enum RuleParamType
{

}