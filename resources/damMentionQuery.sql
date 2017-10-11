﻿select distinct(b.*) from (select docid, sentid, unnest(words) as word from sentences) a left join sentences b on (a.docid=b.docid AND a.sentid=b.sentid) where (lower(a.word) = 'dam' OR lower(a.word) LIKE '%-dam' OR lower(a.word) LIKE 'dam-%') ;