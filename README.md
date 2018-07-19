# Auto-complete-search-engine
A search engine with auto-complete using mapreduce, mapreduce project2
<br/>
idea:
<br/>
Two MR jobs
<br/>
<br/>
MR 1: NGramLibrary<br/>
<br/>
Mapper:<br/>
like word count, output word combination from length 2 to length n, key is word combination val is 1<br/>
Reducer:<br/>
sum the val up for same key<br/>
<br/>
<br/>
MR 2: Load to database and Probility model<br/>
Mapper:<br/>
suppose m words in word combo, output key is first m - 1 words, output val is "the last word" + count, it means the frequency of the last word attached to the previous m - 1 words<br/>
Reducer:<br/>
write to db, key are last word with same previous words<br/>
use a treemap, key is count, val is list of last word
output key is DB(prev m - 1 words, last word, count)<br/>
<br/>

then we can sort count to get freq
