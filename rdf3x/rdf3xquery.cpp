#include "cts/codegen/CodeGen.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "cts/parser/SPARQLLexer.hpp"
#include "cts/parser/SPARQLParser.hpp"
#include "cts/plangen/PlanGen.hpp"
#include "cts/semana/SemanticAnalysis.hpp"
#include "infra/osdep/Timestamp.hpp"
#include "rts/database/Database.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/operator/Operator.hpp"
#include "rts/operator/PlanPrinter.hpp"
#ifdef CONFIG_LINEEDITOR
#include "lineeditor/LineInput.hpp"
#endif
#include <iostream>
#include <fstream>
#include <cstdlib>
#include <chrono>
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
bool smallAddressSpace()
   // Is the address space too small?
{
   return sizeof(void*)<8;
}
//---------------------------------------------------------------------------
static string readInput(istream& in)
   // Read a stream into a string
{
   string result;
   while (true) {
      string s;
      getline(in,s);
      result+=s;
      if (!in.good())
         break;
      result+='\n';
   }
   return result;
}
//---------------------------------------------------------------------------
static bool readLine(string& query)
   // Read a single line
{
#ifdef CONFIG_LINEEDITOR
   // Use the lineeditor interface
   static lineeditor::LineInput editHistory(L">");
   return editHistory.readUtf8(query);
#else
   // Default fallback
   cerr << ">"; cerr.flush();
   return static_cast<bool>(getline(cin,query));
#endif
}
//---------------------------------------------------------------------------
static void showHelp()
   // Show internal commands
{
   cout << "Recognized commands:" << endl
        << "help          shows this help" << endl
        << "select ...    runs a SPARQL query" << endl
        << "explain ...   shows the execution plan for a SPARQL query" << endl
        << "exit          exits the query interface" << endl;
}
//---------------------------------------------------------------------------
static void runQuery(Database& db,const string& query,bool explain, PlanGen::Method method)
   // Evaluate a query
{
   QueryGraph queryGraph;
   {
      // Parse the query
      SPARQLLexer lexer(query);
      SPARQLParser parser(lexer);
      try {
         parser.parse();
      } catch (const SPARQLParser::ParserException& e) {
         cerr << "parse error: " << e.message << endl;
         return;
      }

      // And perform the semantic anaylsis
      try {
         SemanticAnalysis semana(db);
         semana.transform(parser,queryGraph);
      } catch (const SemanticAnalysis::SemanticException& e) {
         cerr << "semantic error: " << e.message << endl;
         return;
      }
      if (queryGraph.knownEmpty()) {
         if (explain)
            cerr << "static analysis determined that the query result will be empty" << endl; else
            cout << "<empty result>" << endl;
         return;
      }
   }

   // Run the optimizer
   PlanGen plangen (method);
   Plan* plan=plangen.translate(db,queryGraph);
   //plan->print(6);
   if (!plan) {
      cerr << "internal error plan generation failed" << endl;
      return;
   }

   // Explain if requested
   if (explain) {
      // Build a physical plan
      Runtime runtime(db);
      Operator* operatorTree=CodeGen().translate(runtime,queryGraph,plan,true);
      DebugPlanPrinter out(runtime,false);
      operatorTree->print(out);
      delete operatorTree;
   } else {
      Runtime runtime(db);
      int min = std::numeric_limits<int>::max();
      for (int i = 0; i < 5; ++i) {
         // track the elapsed time of plan
         Operator* operatorTree = CodeGen().translate(runtime,queryGraph,plan,true);
         std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

         // Else execute it
         if (operatorTree->first()) {
            while (operatorTree->next()) ;
         }

         std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
         // std::cerr << "Time = " << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() << "[ms]" << std::endl;
         int time = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
         delete operatorTree;
         if (time < min) min = time;
      }
      cerr << "Time = " << min << "[ms]" << endl;
      return;
   }

}
//---------------------------------------------------------------------------
PlanGen::Method getMethod(char* mChar) {
   string m(mChar);
   PlanGen::Method method;
   if (m == "rdf3x") {
      method = PlanGen::Method::rdf3x;
   } else if (m == "trueCard") {
      method = PlanGen::Method::trueCard;
   } else if (m == "worst") {
      method = PlanGen::Method::worst;
   } else if (m == "minMin") {
      method = PlanGen::Method::minMin;
   } else if (m == "allMin") {
      method = PlanGen::Method::allMin;
   } else if (m == "maxMin") {
      method = PlanGen::Method::maxMin;
   } else if (m == "minAvg") {
      method = PlanGen::Method::minAvg;
   } else if (m == "allAvg") {
      method = PlanGen::Method::allAvg;
   } else if (m == "maxAvg") {
      method = PlanGen::Method::maxAvg;
   } else if (m == "minMax") {
      method = PlanGen::Method::minMax;
   } else if (m == "allMax") {
      method = PlanGen::Method::allMax;
   } else if (m == "maxMax") {
      method = PlanGen::Method::maxMax;
   } else if (m == "baselineMinMin") {
      method = PlanGen::Method::baselineMinMin;
   } else if (m == "midEdgeMaxMax") {
      method = PlanGen::Method::midEdgeMaxMax;
   } else if (m == "trigMaxMax") {
      method = PlanGen::Method::trigMaxMax;
   } else if (m == "extMaxMax") {
      method = PlanGen::Method::extMaxMax;
   } else if (m == "trigExtMaxMax") {
      method = PlanGen::Method::trigExtMaxMax;
   } else {
      cerr << "Unrecognized method " << m << endl;
      exit(1);
   }

   return method;
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   // Warn first
   if (smallAddressSpace())
      cout << "Warning: Running RDF-3X on a 32 bit system is not supported and will fail for large data sets. Please use a 64 bit system instead!" << endl;

   // Greeting
   cout << "RDF-3X query interface" << endl
        << "(c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x" << endl
				<< "(c) 2013 Hancel Gonzalez and Giuseppe De Simone. Web site: http://github.com/gh-rdf3x/gh-rdf3x" << endl;

   // Check the arguments
   if ((argc!=2)&&(argc!=3) && (argc != 4)) {
      cerr << "usage: " << argv[0] << " <database> [queryfile] [method]" << endl;
      return 1;
   }

   // Open the database
   Database db;
   if (!db.open(argv[1],true)) {
      cerr << "unable to open database " << argv[1] << endl;
      return 1;
   }

   PlanGen::Method method;
   if (argc == 4) method = getMethod(argv[3]);
   else method = PlanGen::Method::rdf3x;
   // Execute a single query?
   if (argc==3 || argc == 4) {
      ifstream in(argv[2]);
      if (!in.is_open()) {
         cerr << "unable to open " << argv[2] << endl;
         return 1;
      }
      string query=readInput(in);
      if (query.substr(0,8)=="explain ") {
         runQuery(db,query.substr(8),true, method);
      } else {
         runQuery(db,query,false, method);
      }
   } else {
      // No, accept user input
      cerr << "Enter 'help' for instructions" << endl;
      while (true) {
         string query;
         if (!readLine(query))
            break;
         if (query=="") continue;

         if ((query=="quit")||(query=="exit")) {
            break;
         } else if (query=="help") {
            showHelp();
         } else if (query.substr(0,8)=="explain ") {
            runQuery(db,query.substr(8),true, PlanGen::Method::rdf3x);
         } else {
            runQuery(db,query,false, PlanGen::Method::rdf3x);
         }
         cout.flush();
      }
   }
}
//---------------------------------------------------------------------------
