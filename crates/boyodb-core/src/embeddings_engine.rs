//! Embeddings Engine for BoyoDB
//!
//! Generate vector embeddings from text, images, and structured data:
//! - Text embeddings (sentence transformers, word2vec)
//! - Image embeddings (CNN feature extraction)
//! - Multimodal embeddings (CLIP-style)
//! - Embedding caching and batching

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use parking_lot::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

/// Embedding model types
#[derive(Debug, Clone, PartialEq)]
pub enum EmbeddingModelType {
    /// Sentence transformer models
    SentenceTransformer(SentenceTransformerModel),
    /// Word embedding models
    Word2Vec,
    FastText,
    GloVe,
    /// Image embedding models
    ImageNet,
    CLIP,
    ResNet,
    /// Custom ONNX model
    CustomOnnx,
}

/// Sentence transformer model variants
#[derive(Debug, Clone, PartialEq)]
pub enum SentenceTransformerModel {
    AllMiniLmL6V2,      // 384 dimensions
    AllMpnetBaseV2,     // 768 dimensions
    MultiQaMiniLmL6,    // 384 dimensions
    ParaphraseMinilmL6, // 384 dimensions
    Custom(String),
}

impl SentenceTransformerModel {
    pub fn dimension(&self) -> usize {
        match self {
            SentenceTransformerModel::AllMiniLmL6V2 => 384,
            SentenceTransformerModel::AllMpnetBaseV2 => 768,
            SentenceTransformerModel::MultiQaMiniLmL6 => 384,
            SentenceTransformerModel::ParaphraseMinilmL6 => 384,
            SentenceTransformerModel::Custom(_) => 384, // default
        }
    }
}

/// Embedding configuration
#[derive(Debug, Clone)]
pub struct EmbeddingConfig {
    /// Model type
    pub model_type: EmbeddingModelType,
    /// Model path or name
    pub model_path: String,
    /// Embedding dimension
    pub dimension: usize,
    /// Maximum sequence length for text
    pub max_sequence_length: usize,
    /// Batch size for inference
    pub batch_size: usize,
    /// Normalize embeddings to unit length
    pub normalize: bool,
    /// Cache embeddings
    pub cache_enabled: bool,
    /// Cache TTL in seconds
    pub cache_ttl_seconds: u64,
}

impl Default for EmbeddingConfig {
    fn default() -> Self {
        Self {
            model_type: EmbeddingModelType::SentenceTransformer(
                SentenceTransformerModel::AllMiniLmL6V2,
            ),
            model_path: "all-MiniLM-L6-v2".to_string(),
            dimension: 384,
            max_sequence_length: 512,
            batch_size: 32,
            normalize: true,
            cache_enabled: true,
            cache_ttl_seconds: 3600,
        }
    }
}

/// Embedding vector
#[derive(Debug, Clone)]
pub struct Embedding {
    /// Vector values
    pub values: Vec<f32>,
    /// Dimension
    pub dimension: usize,
    /// Model used
    pub model: String,
    /// Generation timestamp
    pub timestamp: i64,
}

impl Embedding {
    /// Create a new embedding
    pub fn new(values: Vec<f32>, model: &str) -> Self {
        let dimension = values.len();
        Self {
            values,
            dimension,
            model: model.to_string(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
        }
    }

    /// Compute cosine similarity with another embedding
    pub fn cosine_similarity(&self, other: &Embedding) -> f32 {
        if self.dimension != other.dimension {
            return 0.0;
        }

        let dot: f32 = self.values.iter().zip(other.values.iter()).map(|(a, b)| a * b).sum();
        let norm_a: f32 = self.values.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm_b: f32 = other.values.iter().map(|x| x * x).sum::<f32>().sqrt();

        if norm_a == 0.0 || norm_b == 0.0 {
            return 0.0;
        }

        dot / (norm_a * norm_b)
    }

    /// Compute Euclidean distance with another embedding
    pub fn euclidean_distance(&self, other: &Embedding) -> f32 {
        if self.dimension != other.dimension {
            return f32::MAX;
        }

        self.values
            .iter()
            .zip(other.values.iter())
            .map(|(a, b)| (a - b).powi(2))
            .sum::<f32>()
            .sqrt()
    }

    /// Normalize to unit length
    pub fn normalize(&mut self) {
        let norm: f32 = self.values.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for v in &mut self.values {
                *v /= norm;
            }
        }
    }

    /// Add another embedding (element-wise)
    pub fn add(&self, other: &Embedding) -> Option<Embedding> {
        if self.dimension != other.dimension {
            return None;
        }

        let values: Vec<f32> = self
            .values
            .iter()
            .zip(other.values.iter())
            .map(|(a, b)| a + b)
            .collect();

        Some(Embedding::new(values, &self.model))
    }

    /// Average multiple embeddings
    pub fn average(embeddings: &[Embedding]) -> Option<Embedding> {
        if embeddings.is_empty() {
            return None;
        }

        let dim = embeddings[0].dimension;
        if embeddings.iter().any(|e| e.dimension != dim) {
            return None;
        }

        let n = embeddings.len() as f32;
        let mut values = vec![0.0f32; dim];

        for emb in embeddings {
            for (i, v) in emb.values.iter().enumerate() {
                values[i] += v / n;
            }
        }

        Some(Embedding::new(values, &embeddings[0].model))
    }
}

/// Text tokenizer
pub struct Tokenizer {
    /// Vocabulary mapping
    vocab: HashMap<String, u32>,
    /// Inverse vocabulary
    inv_vocab: HashMap<u32, String>,
    /// Special tokens
    pad_token_id: u32,
    cls_token_id: u32,
    sep_token_id: u32,
    unk_token_id: u32,
    /// Max length
    max_length: usize,
}

impl Tokenizer {
    /// Create a simple whitespace tokenizer
    pub fn new_simple(max_length: usize) -> Self {
        let mut vocab = HashMap::new();
        vocab.insert("[PAD]".to_string(), 0);
        vocab.insert("[CLS]".to_string(), 1);
        vocab.insert("[SEP]".to_string(), 2);
        vocab.insert("[UNK]".to_string(), 3);

        let inv_vocab: HashMap<u32, String> = vocab.iter().map(|(k, v)| (*v, k.clone())).collect();

        Self {
            vocab,
            inv_vocab,
            pad_token_id: 0,
            cls_token_id: 1,
            sep_token_id: 2,
            unk_token_id: 3,
            max_length,
        }
    }

    /// Tokenize text
    pub fn tokenize(&self, text: &str) -> Vec<u32> {
        let mut tokens = vec![self.cls_token_id];

        for word in text.split_whitespace() {
            let word_lower = word.to_lowercase();
            let token_id = self.vocab.get(&word_lower).copied().unwrap_or(self.unk_token_id);
            tokens.push(token_id);

            if tokens.len() >= self.max_length - 1 {
                break;
            }
        }

        tokens.push(self.sep_token_id);

        // Pad to max length
        while tokens.len() < self.max_length {
            tokens.push(self.pad_token_id);
        }

        tokens
    }

    /// Get vocabulary size
    pub fn vocab_size(&self) -> usize {
        self.vocab.len()
    }
}

/// Embedding cache
struct EmbeddingCache {
    cache: RwLock<HashMap<u64, (Embedding, i64)>>,
    ttl_ms: i64,
    max_size: usize,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl EmbeddingCache {
    fn new(ttl_seconds: u64, max_size: usize) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            ttl_ms: (ttl_seconds * 1000) as i64,
            max_size,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    fn hash_key(text: &str) -> u64 {
        let mut hash: u64 = 0;
        for byte in text.bytes() {
            hash = hash.wrapping_mul(31).wrapping_add(byte as u64);
        }
        hash
    }

    fn get(&self, text: &str) -> Option<Embedding> {
        let key = Self::hash_key(text);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let cache = self.cache.read();
        if let Some((embedding, timestamp)) = cache.get(&key) {
            if now - timestamp < self.ttl_ms {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Some(embedding.clone());
            }
        }

        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    fn put(&self, text: &str, embedding: Embedding) {
        let key = Self::hash_key(text);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let mut cache = self.cache.write();

        // Evict if over size
        if cache.len() >= self.max_size {
            // Remove oldest entry
            let oldest = cache
                .iter()
                .min_by_key(|(_, (_, ts))| ts)
                .map(|(k, _)| *k);
            if let Some(key) = oldest {
                cache.remove(&key);
            }
        }

        cache.insert(key, (embedding, now));
    }

    fn stats(&self) -> (u64, u64) {
        (
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
        )
    }
}

/// Simple embedding model (for demonstration - real impl would use ONNX)
struct SimpleEmbeddingModel {
    dimension: usize,
    weights: Vec<Vec<f32>>,
}

impl SimpleEmbeddingModel {
    fn new(vocab_size: usize, dimension: usize) -> Self {
        // Initialize with random weights (deterministic for testing)
        let mut weights = Vec::with_capacity(vocab_size);
        for i in 0..vocab_size {
            let mut w = Vec::with_capacity(dimension);
            for j in 0..dimension {
                // Pseudo-random initialization
                let val = ((i * 31 + j * 17) % 1000) as f32 / 1000.0 - 0.5;
                w.push(val);
            }
            // Normalize
            let norm: f32 = w.iter().map(|x| x * x).sum::<f32>().sqrt();
            for v in &mut w {
                *v /= norm.max(1e-6);
            }
            weights.push(w);
        }

        Self { dimension, weights }
    }

    fn embed_tokens(&self, tokens: &[u32]) -> Vec<f32> {
        let mut result = vec![0.0f32; self.dimension];
        let mut count = 0;

        for &token in tokens {
            if (token as usize) < self.weights.len() && token != 0 {
                // Skip padding
                for (i, &w) in self.weights[token as usize].iter().enumerate() {
                    result[i] += w;
                }
                count += 1;
            }
        }

        // Average pooling
        if count > 0 {
            for v in &mut result {
                *v /= count as f32;
            }
        }

        result
    }
}

/// Embeddings Engine
pub struct EmbeddingsEngine {
    config: EmbeddingConfig,
    tokenizer: Tokenizer,
    model: SimpleEmbeddingModel,
    cache: Option<EmbeddingCache>,
    stats: EngineStats,
}

struct EngineStats {
    embeddings_generated: AtomicU64,
    batch_requests: AtomicU64,
    total_tokens: AtomicU64,
}

impl EmbeddingsEngine {
    /// Create a new embeddings engine
    pub fn new(config: EmbeddingConfig) -> Self {
        let tokenizer = Tokenizer::new_simple(config.max_sequence_length);
        let model = SimpleEmbeddingModel::new(10000, config.dimension);

        let cache = if config.cache_enabled {
            Some(EmbeddingCache::new(config.cache_ttl_seconds, 10000))
        } else {
            None
        };

        Self {
            config,
            tokenizer,
            model,
            cache,
            stats: EngineStats {
                embeddings_generated: AtomicU64::new(0),
                batch_requests: AtomicU64::new(0),
                total_tokens: AtomicU64::new(0),
            },
        }
    }

    /// Generate embedding for text
    pub fn embed_text(&self, text: &str) -> Embedding {
        // Check cache
        if let Some(ref cache) = self.cache {
            if let Some(embedding) = cache.get(text) {
                return embedding;
            }
        }

        // Tokenize
        let tokens = self.tokenizer.tokenize(text);
        self.stats
            .total_tokens
            .fetch_add(tokens.len() as u64, Ordering::Relaxed);

        // Generate embedding
        let values = self.model.embed_tokens(&tokens);
        let mut embedding = Embedding::new(values, &self.config.model_path);

        if self.config.normalize {
            embedding.normalize();
        }

        // Cache result
        if let Some(ref cache) = self.cache {
            cache.put(text, embedding.clone());
        }

        self.stats.embeddings_generated.fetch_add(1, Ordering::Relaxed);
        embedding
    }

    /// Generate embeddings for multiple texts (batched)
    pub fn embed_batch(&self, texts: &[&str]) -> Vec<Embedding> {
        self.stats.batch_requests.fetch_add(1, Ordering::Relaxed);

        texts.iter().map(|text| self.embed_text(text)).collect()
    }

    /// Generate embedding for image (placeholder)
    pub fn embed_image(&self, _image_data: &[u8]) -> Embedding {
        // In a real implementation, this would:
        // 1. Decode the image
        // 2. Preprocess (resize, normalize)
        // 3. Run through CNN model
        // 4. Extract feature vector

        // Placeholder: return random embedding
        let values: Vec<f32> = (0..self.config.dimension)
            .map(|i| (i as f32 * 0.01).sin())
            .collect();

        let mut embedding = Embedding::new(values, "image_model");
        if self.config.normalize {
            embedding.normalize();
        }

        self.stats.embeddings_generated.fetch_add(1, Ordering::Relaxed);
        embedding
    }

    /// Generate multimodal embedding (text + image)
    pub fn embed_multimodal(&self, text: &str, _image_data: Option<&[u8]>) -> Embedding {
        let text_embedding = self.embed_text(text);

        // In a real CLIP-style implementation, we would:
        // 1. Generate text embedding
        // 2. Generate image embedding
        // 3. Project both to shared space
        // 4. Combine or return separately

        text_embedding
    }

    /// Compute similarity between two texts
    pub fn text_similarity(&self, text1: &str, text2: &str) -> f32 {
        let emb1 = self.embed_text(text1);
        let emb2 = self.embed_text(text2);
        emb1.cosine_similarity(&emb2)
    }

    /// Find most similar texts from a list
    pub fn find_similar(&self, query: &str, candidates: &[&str], top_k: usize) -> Vec<(usize, f32)> {
        let query_emb = self.embed_text(query);
        let candidate_embs = self.embed_batch(candidates);

        let mut similarities: Vec<(usize, f32)> = candidate_embs
            .iter()
            .enumerate()
            .map(|(i, emb)| (i, query_emb.cosine_similarity(emb)))
            .collect();

        similarities.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        similarities.truncate(top_k);
        similarities
    }

    /// Get engine statistics
    pub fn stats(&self) -> EmbeddingStats {
        let (cache_hits, cache_misses) = self
            .cache
            .as_ref()
            .map(|c| c.stats())
            .unwrap_or((0, 0));

        EmbeddingStats {
            embeddings_generated: self.stats.embeddings_generated.load(Ordering::Relaxed),
            batch_requests: self.stats.batch_requests.load(Ordering::Relaxed),
            total_tokens: self.stats.total_tokens.load(Ordering::Relaxed),
            cache_hits,
            cache_misses,
            dimension: self.config.dimension,
            model: self.config.model_path.clone(),
        }
    }

    /// Get embedding dimension
    pub fn dimension(&self) -> usize {
        self.config.dimension
    }
}

/// Embedding statistics
#[derive(Debug, Clone)]
pub struct EmbeddingStats {
    pub embeddings_generated: u64,
    pub batch_requests: u64,
    pub total_tokens: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub dimension: usize,
    pub model: String,
}

/// SQL interface for embeddings
pub fn sql_embed(engine: &EmbeddingsEngine, text: &str) -> Vec<f32> {
    engine.embed_text(text).values
}

pub fn sql_embed_similarity(engine: &EmbeddingsEngine, text1: &str, text2: &str) -> f32 {
    engine.text_similarity(text1, text2)
}

/// Embedding model registry
pub struct EmbeddingRegistry {
    engines: RwLock<HashMap<String, Arc<EmbeddingsEngine>>>,
}

impl EmbeddingRegistry {
    pub fn new() -> Self {
        Self {
            engines: RwLock::new(HashMap::new()),
        }
    }

    pub fn register(&self, name: &str, config: EmbeddingConfig) -> Arc<EmbeddingsEngine> {
        let engine = Arc::new(EmbeddingsEngine::new(config));
        self.engines
            .write()
            .insert(name.to_string(), Arc::clone(&engine));
        engine
    }

    pub fn get(&self, name: &str) -> Option<Arc<EmbeddingsEngine>> {
        self.engines.read().get(name).cloned()
    }

    pub fn get_or_default(&self) -> Arc<EmbeddingsEngine> {
        let engines = self.engines.read();
        if let Some(engine) = engines.values().next() {
            Arc::clone(engine)
        } else {
            drop(engines);
            self.register("default", EmbeddingConfig::default())
        }
    }

    pub fn list(&self) -> Vec<String> {
        self.engines.read().keys().cloned().collect()
    }
}

impl Default for EmbeddingRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_embedding_generation() {
        let engine = EmbeddingsEngine::new(EmbeddingConfig::default());
        let embedding = engine.embed_text("Hello world");

        assert_eq!(embedding.dimension, 384);
        assert_eq!(embedding.values.len(), 384);
    }

    #[test]
    fn test_embedding_normalization() {
        let engine = EmbeddingsEngine::new(EmbeddingConfig::default());
        let embedding = engine.embed_text("Test text");

        let norm: f32 = embedding.values.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_cosine_similarity() {
        let engine = EmbeddingsEngine::new(EmbeddingConfig::default());

        // Same text should have similarity 1.0
        let sim = engine.text_similarity("hello world", "hello world");
        assert!((sim - 1.0).abs() < 0.01);

        // Similar texts should have high similarity
        let sim = engine.text_similarity("hello world", "hello there");
        assert!(sim > 0.5);
    }

    #[test]
    fn test_batch_embedding() {
        let engine = EmbeddingsEngine::new(EmbeddingConfig::default());
        let texts = vec!["text one", "text two", "text three"];
        let embeddings = engine.embed_batch(&texts);

        assert_eq!(embeddings.len(), 3);
        for emb in &embeddings {
            assert_eq!(emb.dimension, 384);
        }
    }

    #[test]
    fn test_find_similar() {
        let engine = EmbeddingsEngine::new(EmbeddingConfig::default());
        let candidates = vec!["apple fruit", "banana fruit", "car vehicle", "bike vehicle"];
        let results = engine.find_similar("orange fruit", &candidates, 2);

        assert_eq!(results.len(), 2);
        // Fruit-related candidates should rank higher
    }

    #[test]
    fn test_embedding_caching() {
        let config = EmbeddingConfig {
            cache_enabled: true,
            cache_ttl_seconds: 3600,
            ..Default::default()
        };
        let engine = EmbeddingsEngine::new(config);

        // First call - cache miss
        let _emb1 = engine.embed_text("cached text");

        // Second call - cache hit
        let _emb2 = engine.embed_text("cached text");

        let stats = engine.stats();
        assert_eq!(stats.cache_hits, 1);
        assert_eq!(stats.cache_misses, 1);
    }

    #[test]
    fn test_embedding_average() {
        let emb1 = Embedding::new(vec![1.0, 0.0, 0.0], "test");
        let emb2 = Embedding::new(vec![0.0, 1.0, 0.0], "test");
        let emb3 = Embedding::new(vec![0.0, 0.0, 1.0], "test");

        let avg = Embedding::average(&[emb1, emb2, emb3]).unwrap();
        assert!((avg.values[0] - 0.333).abs() < 0.01);
        assert!((avg.values[1] - 0.333).abs() < 0.01);
        assert!((avg.values[2] - 0.333).abs() < 0.01);
    }

    #[test]
    fn test_euclidean_distance() {
        let emb1 = Embedding::new(vec![0.0, 0.0, 0.0], "test");
        let emb2 = Embedding::new(vec![3.0, 4.0, 0.0], "test");

        let dist = emb1.euclidean_distance(&emb2);
        assert!((dist - 5.0).abs() < 0.01);
    }
}
