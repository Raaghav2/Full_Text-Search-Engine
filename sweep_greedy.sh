# For the script to work, changes to the Searcher.java and App.java need to be made to allow direct parameter setting.
# These changes were not included in the code, because of the automation of leaderboard evaluation, they were made only locally for the
# purpose of parameter sweeping.
# Usage: ./sweep_greedy.sh [--dry-run] [--limit N]

set -euo pipefail

JAR="target/cs7is3-search-1.0.0.jar"
INDEX="index"
TOPICS="topics"
QRELS="qrels.assignment2.part1"
TREC_EVAL="/mnt/c/Users/Milena/Desktop/newIR/trec-eval/trec_eval"
OUTPUT_DIR="sweep_results_isolated"
RESULTS_CSV="$OUTPUT_DIR/results_greedy.csv"
LOG="$OUTPUT_DIR/sweep_greedy.log"

TIMEOUT_SECS=300
DRY_RUN=0
LIMIT=0
NEUTRAL_BOOST="1.0"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry|--dry-run)
      DRY_RUN=1
      TIMEOUT_SECS=120
      shift
      ;;
    --limit)
      LIMIT="$2"
      shift 2
      ;;
    -h|--help)
      echo "Usage: $0 [--dry-run] [--limit N]"
      exit 0
      ;;
    *)
      echo "Unknown arg: $1" >&2
      exit 1
      ;;
  esac
done

mkdir -p "$OUTPUT_DIR"

log_msg() {
  ts=$(date '+%Y-%m-%d %H:%M:%S')
  echo "[$ts] $*" | tee -a "$LOG"
}

ANALYZERS=(custom kstem english)
MODELS=(hybrid bm25 lmrdirichlet)
TITLE_BOOSTS=(0.5 1.0 1.5 2.0 2.5 3.0)
DESC_BOOSTS=(0.5 1.0 1.5 2.0 2.5 3.0)
NARR_BOOSTS=(0.5 1.0 1.5 2.0 2.5 3.0)
TOPDOCS=(20 40 60 80 100)
WEIGHT_MULT=(1.25 1.5 1.75 2.0 2.25 2.5)
NUM_EXP=(20 40 60 80 100)
DEFAULT_ANALYZER="english"
DEFAULT_MODEL="bm25"
DEFAULT_TITLE_BOOST="3.0"
DEFAULT_DESC_BOOST="1.3"
DEFAULT_NARRATIVE_BOOST="0.5"
DEFAULT_TOP_DOCS_PILOT="20"
DEFAULT_WEIGHT_MULTIPLIER="1.25"
DEFAULT_NUM_EXPANSIONS="40"

if [ ! -x "$TREC_EVAL" ]; then
  log_msg "WARNING: trec_eval not executable at $TREC_EVAL; metrics will be zeros in dry-run or if missing."
  TREC_EVAL="echo"
fi

if [ ! -f "$JAR" ]; then
  log_msg "Building project..."
  mvn clean package -q || { log_msg "Maven build failed"; exit 1; }
fi

if [ ! -d "$INDEX" ]; then
  log_msg "Creating index..."
  java -jar "$JAR" index --docs "Assignment Two" --index "$INDEX" || { log_msg "Index creation failed"; exit 1; }
fi

csv_init() {
  echo "Parameter,Value,Analyzer,Model,TitleBoost,DescBoost,NarrativeBoost,TopDocsPilot,WeightMultiplier,NumExpansions,MAP,P@20,nDCG@20,RunFile" > "$RESULTS_CSV"
}

get_metrics() {
  local runfile="$1"
  if [ "$TREC_EVAL" = "echo" ]; then
    echo "0.00,0.0000,0.0000"
    return
  fi
  local out
  out=$("$TREC_EVAL" -m map -m P.20 -m ndcg_cut.20 "$QRELS" "$runfile" 2>/dev/null || true)
  local map_raw=0 p20=0 ndcg=0
  while IFS= read -r line; do
    [[ $line =~ ^map.*all[[:space:]]+([0-9.]+) ]] && map_raw="${BASH_REMATCH[1]}"
    [[ $line =~ ^P_20.*all[[:space:]]+([0-9.]+) ]] && p20="${BASH_REMATCH[1]}"
    [[ $line =~ ^ndcg_cut_20.*all[[:space:]]+([0-9.]+) ]] && ndcg="${BASH_REMATCH[1]}"
  done <<< "$out"
  local map=$(awk -v m="$map_raw" 'BEGIN { printf "%.2f", m }')
  echo "$map,$p20,$ndcg"
}

choose_best() {
  local param_name="$1"; shift
  local candidates=("$@")
  local best_val="${candidates[0]}"
  local best_map=-1
  local best_p20=-1
  local best_ndcg=-1

  for val in "${candidates[@]}"; do
    log_msg "Testing $param_name = $val"
    runname="${param_name}_${val}"
    runfile="$OUTPUT_DIR/${runname}.run"

    a="$DEFAULT_ANALYZER"; m="$DEFAULT_MODEL"; tb="$DEFAULT_TITLE_BOOST"; db="$DEFAULT_DESC_BOOST";
    nb="$DEFAULT_NARRATIVE_BOOST"; tdp="$DEFAULT_TOP_DOCS_PILOT"; wm="$DEFAULT_WEIGHT_MULTIPLIER"; ne="$DEFAULT_NUM_EXPANSIONS"
    
    case "$param_name" in
      Analyzer) a="$val" ;;
      Model) m="$val" ;;
      TitleBoost) tb="$val" ;;
      DescBoost) db="$val" ;;
      NarrativeBoost) nb="$val" ;;
      TopDocsPilot) tdp="$val" ;;
      WeightMultiplier) wm="$val" ;;
      NumExpansions) ne="$val" ;;
    esac

    if [[ "$param_name" == "Analyzer" || "$param_name" == "Model" ]]; then
      log_msg "   -> Neutralizing TitleBoost, DescBoost, and NarrativeBoost to $NEUTRAL_BOOST for isolation."
      tb="$NEUTRAL_BOOST"
      db="$NEUTRAL_BOOST"
      nb="$NEUTRAL_BOOST"
    fi

    run_numDocs=1000; run_timeout=$TIMEOUT_SECS

    if [ "$DRY_RUN" -eq 1 ]; then run_numDocs=200; run_timeout=60; fi

    log_msg "Running: analyzer=$a model=$m tb=$tb db=$db nb=$nb tdp=$tdp wm=$wm ne=$ne"
    timeout "$run_timeout" java -jar "$JAR" search --index "$INDEX" --topics "$TOPICS" --output "$runfile" --numDocs "$run_numDocs" \
      --analyzer "$a" --model "$m" --titleBoost "$tb" --descBoost "$db" --narrativeBoost "$nb" \
      --topDocsPilot "$tdp" --weightMultiplier "$wm" --numExpansions "$ne" >/dev/null 2>&1 || true

    if [ ! -f "$runfile" ]; then
      map=0.00; p20=0.0000; ndcg=0.0000
    else
      metrics=$(get_metrics "$runfile")
      IFS=',' read -r map p20 ndcg <<< "$metrics"
    fi

    echo "$param_name,$val,$a,$m,$tb,$db,$nb,$tdp,$wm,$ne,$map,$p20,$ndcg,$runfile" >> "$RESULTS_CSV"

    better=$(awk -v map="$map" -v best_map="$best_map" \
                 -v p20="$p20" -v best_p20="$best_p20" \
                 -v ndcg="$ndcg" -v best_ndcg="$best_ndcg" '
      BEGIN {
        # Convert strings to numbers
        map += 0; best_map += 0; p20 += 0; best_p20 += 0; ndcg += 0; best_ndcg += 0;
        
        if (map > best_map) {
          print 1
        } else if (map == best_map) {
          if (p20 > best_p20) {
            print 1
          } else if (p20 == best_p20) {
            if (ndcg > best_ndcg) {
              print 1
            } else {
              print 0
            }
          } else {
            print 0
          }
        } else {
          print 0
        }
      }
    ')
    
    if [ "$better" -eq 1 ]; then
      best_map="$map"; best_p20="$p20"; best_ndcg="$ndcg"; best_val="$val"
    fi
  done

  log_msg "Selected for $param_name: $best_val (MAP=$best_map P@20=$best_p20 nDCG@20=$best_ndcg)"

  case "$param_name" in
    Analyzer) DEFAULT_ANALYZER="$best_val" ;;
    Model) DEFAULT_MODEL="$best_val" ;;
    TitleBoost) DEFAULT_TITLE_BOOST="$best_val" ;;
    DescBoost) DEFAULT_DESC_BOOST="$best_val" ;;
    NarrativeBoost) DEFAULT_NARRATIVE_BOOST="$best_val" ;;
    TopDocsPilot) DEFAULT_TOP_DOCS_PILOT="$best_val" ;;
    WeightMultiplier) DEFAULT_WEIGHT_MULTIPLIER="$best_val" ;;
    NumExpansions) DEFAULT_NUM_EXPANSIONS="$best_val" ;;
  esac
}


csv_init

log_msg "Optimizing Analyzer (1/8)"
choose_best Analyzer "${ANALYZERS[@]}"
if [ "$LIMIT" -gt 0 ] && [ "$LIMIT" -le 1 ]; then log_msg "Limit reached; exiting."; exit 0; fi

log_msg "Optimizing Model (2/8)"
choose_best Model "${MODELS[@]}"
if [ "$LIMIT" -gt 0 ] && [ "$LIMIT" -le 2 ]; then log_msg "Limit reached; exiting."; exit 0; fi

log_msg "Optimizing TitleBoost (3/8)"
choose_best TitleBoost "${TITLE_BOOSTS[@]}"
if [ "$LIMIT" -gt 0 ] && [ "$LIMIT" -le 3 ]; then log_msg "Limit reached; exiting."; exit 0; fi

log_msg "Optimizing DescBoost (4/8)"
choose_best DescBoost "${DESC_BOOSTS[@]}"
if [ "$LIMIT" -gt 0 ] && [ "$LIMIT" -le 4 ]; then log_msg "Limit reached; exiting."; exit 0; fi

log_msg "Optimizing NarrativeBoost (5/8)"
choose_best NarrativeBoost "${NARR_BOOSTS[@]}"
if [ "$LIMIT" -gt 0 ] && [ "$LIMIT" -le 5 ]; then log_msg "Limit reached; exiting."; exit 0; fi

log_msg "Optimizing TopDocsPilot (6/8)"
choose_best TopDocsPilot "${TOPDOCS[@]}"
if [ "$LIMIT" -gt 0 ] && [ "$LIMIT" -le 6 ]; then log_msg "Limit reached; exiting."; exit 0; fi

log_msg "Optimizing WeightMultiplier (7/8)"
choose_best WeightMultiplier "${WEIGHT_MULT[@]}"
if [ "$LIMIT" -gt 0 ] && [ "$LIMIT" -le 7 ]; then log_msg "Limit reached; exiting."; exit 0; fi

log_msg "Optimizing NumExpansions (8/8)"
choose_best NumExpansions "${NUM_EXP[@]}"
if [ "$LIMIT" -gt 0 ] && [ "$LIMIT" -le 8 ]; then log_msg "Limit reached; exiting."; exit 0; fi

echo "Final configuration:"
echo "Analyzer: $DEFAULT_ANALYZER"
echo "Model: $DEFAULT_MODEL"
echo "TitleBoost: $DEFAULT_TITLE_BOOST"
echo "DescBoost: $DEFAULT_DESC_BOOST"
echo "NarrativeBoost: $DEFAULT_NARRATIVE_BOOST"
echo "TopDocsPilot: $DEFAULT_TOP_DOCS_PILOT"
echo "WeightMultiplier: $DEFAULT_WEIGHT_MULTIPLIER"
echo "NumExpansions: $DEFAULT_NUM_EXPANSIONS"

echo "Results saved to $RESULTS_CSV"

exit 0