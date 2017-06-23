import org.apache.spark.graphx.GraphLoader
val graph = GraphLoader.edgeListFile(sc, "/media/sachin/Study/StudyReport/Apache-Giraph-Deployed-on-Apache-Hadoop-master/face_in.txt")
val ranks = graph.pageRank(0.001).vertices

