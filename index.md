# poems import

PUT /poems
{
  "settings": {
    "analysis": {
      "analyzer": {
        "default": {
          "type": "ik_max_word"
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "title": {
        "type": "text",
        "analyzer": "ik_max_word",
        "search_analyzer": "ik_smart"
      },
      "paragraphs": {
        "type": "text",
        "analyzer": "ik_max_word",
         "search_analyzer": "ik_smart"
      },
      "author": {
        "type": "keyword"
      },
      "rhythmic": {
        "type": "keyword"
      },
      "notes": {
        "type": "text",
        "analyzer": "ik_max_word",
         "search_analyzer": "ik_smart"
      },
      "name": {
        "type": "keyword"
      },
      "desc": {
        "type": "text",
        "analyzer": "ik_max_word",
         "search_analyzer": "ik_smart"
      }
    }
  }
}

GET poems/_search
{
  "query": {
    "match_all": {
     
    }
  }
}

DELETE poems

GET poems/_count

GET poems/_search
{
  "query": {
    "match": {
      "author": "李白"
    }
  }
}


