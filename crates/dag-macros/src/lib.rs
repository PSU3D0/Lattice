// TODO
use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{ToTokens, format_ident, quote};
use semver::Version;
use std::collections::HashSet;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{
    Attribute, Expr, ExprLit, Ident, ItemEnum, ItemFn, Lit, LitStr, Macro, Meta, MetaNameValue,
    Path, Result, Token, parse_macro_input, parse_quote, spanned::Spanned,
};

/// Attribute macro for defining workflow nodes.
#[proc_macro_attribute]
pub fn node(attr: TokenStream, item: TokenStream) -> TokenStream {
    expand_node(attr, item, NodeDefaults::node())
}

/// Attribute macro for defining workflow triggers.
#[proc_macro_attribute]
pub fn trigger(attr: TokenStream, item: TokenStream) -> TokenStream {
    expand_node(attr, item, NodeDefaults::trigger())
}

/// Declarative workflow macro producing Flow IR at compile time.
#[proc_macro]
pub fn workflow(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as WorkflowInput);
    match parsed.expand() {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Attribute helper for Flow-value enums used to constrain generics.
#[proc_macro_attribute]
pub fn flow_enum(attr: TokenStream, item: TokenStream) -> TokenStream {
    if !attr.is_empty() {
        let err = syn::Error::new(
            Span::call_site(),
            "#[flow_enum] does not accept attribute arguments",
        );
        return TokenStream::from(err.to_compile_error());
    }

    let item_tokens: TokenStream2 = item.clone().into();
    let mut enum_item = match syn::parse2::<ItemEnum>(item_tokens.clone()) {
        Ok(item) => item,
        Err(_) => {
            let err = syn::Error::new_spanned(
                item_tokens,
                "#[flow_enum] can only be applied to enum definitions",
            );
            return TokenStream::from(err.to_compile_error());
        }
    };

    match ensure_flow_enum_attrs(&mut enum_item) {
        Ok(()) => TokenStream::from(quote!(#enum_item)),
        Err(err) => TokenStream::from(err.to_compile_error()),
    }
}

struct NodeDefaults {
    kind: &'static str,
    effects: EffectLevel,
    determinism: DeterminismLevel,
}

impl NodeDefaults {
    fn node() -> Self {
        Self {
            kind: "Inline",
            effects: EffectLevel::Pure,
            determinism: DeterminismLevel::BestEffort,
        }
    }

    fn trigger() -> Self {
        Self {
            kind: "Trigger",
            effects: EffectLevel::ReadOnly,
            determinism: DeterminismLevel::Strict,
        }
    }
}

#[derive(Copy, Clone, Debug)]
enum EffectLevel {
    Pure,
    ReadOnly,
    Effectful,
}

impl EffectLevel {
    fn parse(value: &str, span: Span) -> Result<Self> {
        match value {
            "Pure" | "pure" => Ok(EffectLevel::Pure),
            "ReadOnly" | "readonly" => Ok(EffectLevel::ReadOnly),
            "Effectful" | "effectful" => Ok(EffectLevel::Effectful),
            other => Err(syn::Error::new(
                span,
                format!("[EFFECT201] unknown effects value `{other}`"),
            )),
        }
    }

    fn to_tokens(self, span: Span) -> TokenStream2 {
        match self {
            EffectLevel::Pure => enum_expr("Effects", "Pure", span),
            EffectLevel::ReadOnly => enum_expr("Effects", "ReadOnly", span),
            EffectLevel::Effectful => enum_expr("Effects", "Effectful", span),
        }
    }

    fn to_runtime(self) -> dag_core::Effects {
        match self {
            EffectLevel::Pure => dag_core::Effects::Pure,
            EffectLevel::ReadOnly => dag_core::Effects::ReadOnly,
            EffectLevel::Effectful => dag_core::Effects::Effectful,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            EffectLevel::Pure => "Pure",
            EffectLevel::ReadOnly => "ReadOnly",
            EffectLevel::Effectful => "Effectful",
        }
    }
}

#[derive(Copy, Clone, Debug)]
enum DeterminismLevel {
    Strict,
    Stable,
    BestEffort,
    Nondeterministic,
}

impl DeterminismLevel {
    fn parse(value: &str, span: Span) -> Result<Self> {
        match value {
            "Strict" | "strict" => Ok(DeterminismLevel::Strict),
            "Stable" | "stable" => Ok(DeterminismLevel::Stable),
            "BestEffort" | "best_effort" | "besteffort" => Ok(DeterminismLevel::BestEffort),
            "Nondeterministic" | "non_deterministic" | "nondet" => {
                Ok(DeterminismLevel::Nondeterministic)
            }
            other => Err(syn::Error::new(
                span,
                format!("[DET301] unknown determinism value `{other}`"),
            )),
        }
    }

    fn to_tokens(self, span: Span) -> TokenStream2 {
        match self {
            DeterminismLevel::Strict => enum_expr("Determinism", "Strict", span),
            DeterminismLevel::Stable => enum_expr("Determinism", "Stable", span),
            DeterminismLevel::BestEffort => enum_expr("Determinism", "BestEffort", span),
            DeterminismLevel::Nondeterministic => {
                enum_expr("Determinism", "Nondeterministic", span)
            }
        }
    }

    fn to_runtime(self) -> dag_core::Determinism {
        match self {
            DeterminismLevel::Strict => dag_core::Determinism::Strict,
            DeterminismLevel::Stable => dag_core::Determinism::Stable,
            DeterminismLevel::BestEffort => dag_core::Determinism::BestEffort,
            DeterminismLevel::Nondeterministic => dag_core::Determinism::Nondeterministic,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            DeterminismLevel::Strict => "Strict",
            DeterminismLevel::Stable => "Stable",
            DeterminismLevel::BestEffort => "BestEffort",
            DeterminismLevel::Nondeterministic => "Nondeterministic",
        }
    }
}

struct ParsedEffects {
    tokens: TokenStream2,
    level: EffectLevel,
}

impl ParsedEffects {
    fn new(level: EffectLevel, span: Span) -> Self {
        Self {
            tokens: level.to_tokens(span),
            level,
        }
    }
}

struct ParsedDeterminism {
    tokens: TokenStream2,
    level: DeterminismLevel,
}

impl ParsedDeterminism {
    fn new(level: DeterminismLevel, span: Span) -> Self {
        Self {
            tokens: level.to_tokens(span),
            level,
        }
    }
}

struct ResourceSpec {
    alias: Ident,
    capability: Path,
    span: Span,
}

impl Parse for ResourceSpec {
    fn parse(input: ParseStream) -> Result<Self> {
        let alias: Ident = input.parse()?;
        let span = alias.span();
        let content;
        syn::parenthesized!(content in input);
        let capability: Path = content.parse()?;
        Ok(ResourceSpec {
            alias,
            capability,
            span,
        })
    }
}

struct ResourceList {
    entries: Vec<ResourceSpec>,
}

impl Parse for ResourceList {
    fn parse(input: ParseStream) -> Result<Self> {
        let punctuated = Punctuated::<ResourceSpec, Token![,]>::parse_terminated(input)?;
        Ok(ResourceList {
            entries: punctuated.into_iter().collect(),
        })
    }
}

struct NodeArgs {
    name: Option<LitStr>,
    summary: Option<LitStr>,
    effects: Option<ParsedEffects>,
    determinism: Option<ParsedDeterminism>,
    kind: Option<TokenStream2>,
    input_schema: Option<LitStr>,
    output_schema: Option<LitStr>,
    resources: Vec<ResourceSpec>,
}

impl NodeArgs {
    fn parse(args: Punctuated<Meta, Token![,]>, defaults: &NodeDefaults) -> Result<Self> {
        let mut parsed = NodeArgs {
            name: None,
            summary: None,
            effects: None,
            determinism: None,
            kind: None,
            input_schema: None,
            output_schema: None,
            resources: Vec::new(),
        };

        for meta in args {
            match meta {
                Meta::NameValue(MetaNameValue { path, value, .. }) => {
                    let ident = path
                        .get_ident()
                        .ok_or_else(|| syn::Error::new(path.span(), "expected identifier"))?;
                    let lit = match value {
                        Expr::Lit(ExprLit { lit, .. }) => lit,
                        _ => return Err(syn::Error::new(value.span(), "expected literal value")),
                    };
                    match ident.to_string().as_str() {
                        "name" => match lit {
                            Lit::Str(s) => parsed.name = Some(s),
                            _ => {
                                return Err(syn::Error::new(
                                    lit.span(),
                                    "name must be string literal",
                                ));
                            }
                        },
                        "summary" => match lit {
                            Lit::Str(s) => parsed.summary = Some(s),
                            _ => {
                                return Err(syn::Error::new(
                                    lit.span(),
                                    "summary must be string literal",
                                ));
                            }
                        },
                        "effects" => parsed.effects = Some(parse_effects(&lit)?),
                        "determinism" => parsed.determinism = Some(parse_determinism(&lit)?),
                        "kind" => parsed.kind = Some(parse_node_kind(&lit)?),
                        "in" => match lit {
                            Lit::Str(s) => parsed.input_schema = Some(s),
                            _ => {
                                return Err(syn::Error::new(
                                    lit.span(),
                                    "in must be string literal",
                                ));
                            }
                        },
                        "out" => match lit {
                            Lit::Str(s) => parsed.output_schema = Some(s),
                            _ => {
                                return Err(syn::Error::new(
                                    lit.span(),
                                    "out must be string literal",
                                ));
                            }
                        },
                        other => {
                            return Err(syn::Error::new(
                                ident.span(),
                                format!("[DAG003] unknown attribute `{other}`"),
                            ));
                        }
                    }
                }
                Meta::List(list) => {
                    let ident = list
                        .path
                        .get_ident()
                        .ok_or_else(|| syn::Error::new(list.path.span(), "expected identifier"))?;
                    match ident.to_string().as_str() {
                        "resources" => {
                            let entries = syn::parse2::<ResourceList>(list.tokens.clone())?.entries;
                            parsed.resources.extend(entries.into_iter());
                        }
                        other => {
                            return Err(syn::Error::new(
                                ident.span(),
                                format!("[DAG003] unknown attribute `{other}`"),
                            ));
                        }
                    }
                }
                Meta::Path(path) => {
                    return Err(syn::Error::new(
                        path.span(),
                        "expected key = \"value\" pairs in attribute",
                    ));
                }
            }
        }

        if parsed.effects.is_none() {
            parsed.effects = Some(ParsedEffects::new(defaults.effects, Span::call_site()));
        }
        if parsed.determinism.is_none() {
            parsed.determinism = Some(ParsedDeterminism::new(
                defaults.determinism,
                Span::call_site(),
            ));
        }
        if parsed.kind.is_none() {
            parsed.kind = Some(enum_expr("NodeKind", defaults.kind, Span::call_site()));
        }

        Ok(parsed)
    }
}

fn expand_node(attr: TokenStream, item: TokenStream, defaults: NodeDefaults) -> TokenStream {
    let args = parse_macro_input!(attr with Punctuated::<Meta, Token![,]>::parse_terminated);
    let function = parse_macro_input!(item as ItemFn);
    match node_impl(args, function, defaults) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn node_impl(
    args: Punctuated<Meta, Token![,]>,
    function: ItemFn,
    defaults: NodeDefaults,
) -> Result<TokenStream2> {
    if function.sig.asyncness.is_none() {
        return Err(syn::Error::new(
            function.sig.span(),
            "[DAG001] #[node] requires an async function",
        ));
    }

    let config = NodeArgs::parse(args, &defaults)?;
    let name_lit = config.name.as_ref().cloned().ok_or_else(|| {
        syn::Error::new(
            function.sig.span(),
            "[DAG001] #[node] requires `name = \"...\"`",
        )
    })?;
    let summary_expr = config
        .summary
        .as_ref()
        .map(|s| quote!(Some(#s)))
        .unwrap_or_else(|| quote!(None));

    let (input_schema_expr, output_schema_expr) = infer_schemas(&function, &config)?;

    let fn_name = &function.sig.ident;
    let spec_ident = format_ident!("{}_NODE_SPEC", fn_name.to_string().to_uppercase());
    let accessor_ident = format_ident!("{}_node_spec", fn_name);

    let effects = config
        .effects
        .as_ref()
        .expect("effects default should be populated");
    let determinism = config
        .determinism
        .as_ref()
        .expect("determinism default should be populated");
    let kind_expr = config
        .kind
        .as_ref()
        .expect("node kind default should be populated");

    let (determinism_hints, effect_hints) = compute_resource_hints(&config.resources);
    validate_effect_hints(&effect_hints, effects)?;
    validate_determinism_hints(&determinism_hints, determinism)?;
    let determinism_hints_expr = hint_array_tokens(&determinism_hints);
    let effect_hints_expr = hint_array_tokens(&effect_hints);
    let effects_expr = &effects.tokens;
    let determinism_expr = &determinism.tokens;

    Ok(quote! {
        #function

        #[allow(non_upper_case_globals)]
        pub const #spec_ident: ::dag_core::NodeSpec = ::dag_core::NodeSpec {
            identifier: concat!(module_path!(), "::", stringify!(#fn_name)),
            name: #name_lit,
            kind: #kind_expr,
            summary: #summary_expr,
            in_schema: #input_schema_expr,
            out_schema: #output_schema_expr,
            effects: #effects_expr,
            determinism: #determinism_expr,
            determinism_hints: #determinism_hints_expr,
            effect_hints: #effect_hints_expr,
        };

        #[allow(dead_code)]
        pub fn #accessor_ident() -> &'static ::dag_core::NodeSpec {
            &#spec_ident
        }
    })
}

struct HintSpec {
    value: String,
    span: Span,
    origin: String,
}

fn compute_resource_hints(resources: &[ResourceSpec]) -> (Vec<HintSpec>, Vec<HintSpec>) {
    let mut determinism = Vec::new();
    let mut effects = Vec::new();
    let mut determinism_seen = HashSet::new();
    let mut effects_seen = HashSet::new();

    for resource in resources {
        let alias = resource.alias.to_string();
        let alias_lower = alias.to_ascii_lowercase();
        let cap_ident = resource
            .capability
            .segments
            .last()
            .map(|segment| segment.ident.to_string())
            .unwrap_or_default();
        let cap_lower = cap_ident.to_ascii_lowercase();
        let namespace = canonical_namespace(&alias_lower, &cap_lower);

        push_determinism_hint(
            &mut determinism,
            &mut determinism_seen,
            &alias,
            &alias_lower,
            &cap_lower,
            resource.span,
        );

        push_effect_hints(
            &mut effects,
            &mut effects_seen,
            &alias,
            &alias_lower,
            &cap_lower,
            &namespace,
            resource.span,
        );
    }

    (determinism, effects)
}

fn canonical_namespace(alias: &str, cap_lower: &str) -> String {
    for domain in ["http", "kv", "db", "queue", "blob"] {
        if alias.contains(domain) || cap_lower.contains(domain) {
            return domain.to_string();
        }
    }
    alias
        .split('_')
        .next()
        .map(|part| part.to_ascii_lowercase())
        .unwrap_or_else(|| alias.to_string())
}

fn push_determinism_hint(
    accumulator: &mut Vec<HintSpec>,
    seen: &mut HashSet<String>,
    alias: &str,
    alias_lower: &str,
    cap_lower: &str,
    span: Span,
) {
    if alias_lower.contains("clock") || cap_lower.contains("clock") {
        push_hint(accumulator, seen, "resource::clock", alias, span);
    }

    if alias_lower.contains("rng")
        || alias_lower.contains("random")
        || cap_lower.contains("rng")
        || cap_lower.contains("random")
    {
        push_hint(accumulator, seen, "resource::rng", alias, span);
    }

    if alias_lower.contains("http") || cap_lower.contains("http") {
        push_hint(accumulator, seen, "resource::http", alias, span);
    }

    if alias_lower.contains("kv") || cap_lower.contains("kv") {
        push_hint(accumulator, seen, "resource::kv", alias, span);
    }

    if alias_lower.contains("db") || cap_lower.contains("db") {
        push_hint(accumulator, seen, "resource::db", alias, span);
    }
}

fn push_effect_hints(
    accumulator: &mut Vec<HintSpec>,
    seen: &mut HashSet<String>,
    alias: &str,
    alias_lower: &str,
    cap_lower: &str,
    namespace: &str,
    span: Span,
) {
    if let Some(hint) = classify_read_hint(alias_lower, cap_lower, namespace) {
        push_hint(accumulator, seen, hint.as_str(), alias, span);
    }

    if let Some(hint) = classify_write_hint(alias_lower, cap_lower, namespace) {
        push_hint(accumulator, seen, hint.as_str(), alias, span);
    }
}

fn classify_read_hint(alias_lower: &str, cap_lower: &str, namespace: &str) -> Option<String> {
    const TOKENS: &[&str] = &["read", "fetch", "load", "get"];
    if TOKENS
        .iter()
        .any(|token| cap_lower.contains(token) || alias_lower.contains(token))
    {
        return Some(format!("resource::{}::read", namespace));
    }
    None
}

fn classify_write_hint(alias_lower: &str, cap_lower: &str, namespace: &str) -> Option<String> {
    const TOKENS: &[&str] = &[
        "write",
        "producer",
        "publish",
        "publisher",
        "sender",
        "send",
        "emit",
        "upsert",
        "insert",
        "delete",
        "update",
        "post",
        "put",
        "patch",
    ];
    if TOKENS
        .iter()
        .any(|token| cap_lower.contains(token) || alias_lower.contains(token))
    {
        return Some(format!("resource::{}::write", namespace));
    }
    None
}

fn push_hint(
    accumulator: &mut Vec<HintSpec>,
    seen: &mut HashSet<String>,
    hint: &str,
    origin: &str,
    span: Span,
) {
    let value = hint.to_string();
    if seen.insert(value.clone()) {
        accumulator.push(HintSpec {
            value,
            span,
            origin: origin.to_string(),
        });
    }
}

fn hint_array_tokens(hints: &[HintSpec]) -> TokenStream2 {
    if hints.is_empty() {
        quote!(&[] as &[&'static str])
    } else {
        let values = hints.iter().map(|hint| {
            let lit = LitStr::new(&hint.value, Span::call_site());
            quote!(#lit)
        });
        quote!(&[#(#values),*] as &[&'static str])
    }
}

fn validate_effect_hints(hints: &[HintSpec], effects: &ParsedEffects) -> Result<()> {
    for hint in hints {
        if let Some(constraint) =
            dag_core::effects_registry::constraint_for_hint(hint.value.as_str())
        {
            if !effects.level.to_runtime().is_at_least(constraint.minimum) {
                return Err(syn::Error::new(
                    hint.span,
                    format!(
                        "[EFFECT201] resource `{}` (from `{}`) requires effects >= {}, but node declares {}. {}",
                        hint.value,
                        hint.origin,
                        constraint.minimum.as_str(),
                        effects.level.as_str(),
                        constraint.guidance,
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn validate_determinism_hints(hints: &[HintSpec], determinism: &ParsedDeterminism) -> Result<()> {
    for hint in hints {
        if let Some(constraint) = dag_core::determinism::constraint_for_hint(hint.value.as_str()) {
            if !determinism
                .level
                .to_runtime()
                .is_at_least(constraint.minimum)
            {
                return Err(syn::Error::new(
                    hint.span,
                    format!(
                        "[DET302] resource `{}` (from `{}`) requires determinism >= {}, but node declares {}. {}",
                        hint.value,
                        hint.origin,
                        constraint.minimum.as_str(),
                        determinism.level.as_str(),
                        constraint.guidance,
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn infer_schemas(function: &ItemFn, config: &NodeArgs) -> Result<(TokenStream2, TokenStream2)> {
    let input_schema = if let Some(custom) = &config.input_schema {
        quote!(::dag_core::SchemaSpec::Named(#custom))
    } else {
        let arg = function.sig.inputs.first().ok_or_else(|| {
            syn::Error::new(
                function.sig.span(),
                "[DAG001] nodes must accept exactly one argument",
            )
        })?;
        let ty = match arg {
            syn::FnArg::Typed(pt) => &pt.ty,
            syn::FnArg::Receiver(_) => {
                return Err(syn::Error::new(
                    arg.span(),
                    "[DAG001] first parameter must be typed (use `fn foo(input: T)`)",
                ));
            }
        };
        schema_from_type(ty)
    };

    let output_schema = if let Some(custom) = &config.output_schema {
        quote!(::dag_core::SchemaSpec::Named(#custom))
    } else {
        let ty = match &function.sig.output {
            syn::ReturnType::Type(_, ty) => ty.as_ref(),
            syn::ReturnType::Default => {
                return Err(syn::Error::new(
                    function.sig.output.span(),
                    "[DAG001] nodes must return dag_core::NodeResult<T>",
                ));
            }
        };
        schema_from_return(ty)?
    };

    Ok((input_schema, output_schema))
}

fn schema_from_type(ty: &syn::Type) -> TokenStream2 {
    let ty_str = ty.to_token_stream().to_string();
    if ty_str == "()" {
        quote!(::dag_core::SchemaSpec::Opaque)
    } else {
        let lit = LitStr::new(&ty_str, ty.span());
        quote!(::dag_core::SchemaSpec::Named(#lit))
    }
}

fn schema_from_return(ty: &syn::Type) -> Result<TokenStream2> {
    if let syn::Type::Path(path) = ty {
        if let Some(last) = path.path.segments.last() {
            if last.ident == "NodeResult" {
                if let syn::PathArguments::AngleBracketed(args) = &last.arguments {
                    if let Some(syn::GenericArgument::Type(inner)) = args.args.first() {
                        return Ok(schema_from_type(inner));
                    }
                }
            }
        }
    }
    Err(syn::Error::new(
        ty.span(),
        "[DAG001] return type must be dag_core::NodeResult<Output>",
    ))
}

fn ensure_flow_enum_attrs(item: &mut ItemEnum) -> Result<()> {
    ensure_flow_enum_derives(item)?;
    ensure_flow_enum_tag(item);
    Ok(())
}

fn ensure_flow_enum_derives(item: &mut ItemEnum) -> Result<()> {
    let required_traits: [(&str, Path); 5] = [
        ("Debug", parse_quote!(Debug)),
        ("Clone", parse_quote!(Clone)),
        ("Serialize", parse_quote!(::serde::Serialize)),
        ("Deserialize", parse_quote!(::serde::Deserialize)),
        ("JsonSchema", parse_quote!(::schemars::JsonSchema)),
    ];

    if let Some(index) = item
        .attrs
        .iter()
        .position(|attr| attr.path().is_ident("derive"))
    {
        let original = item.attrs.remove(index);
        let mut traits: Vec<Path> = original
            .parse_args_with(Punctuated::<Path, Token![,]>::parse_terminated)?
            .into_iter()
            .collect();

        for (ident, path) in &required_traits {
            let already_present = traits.iter().any(|existing| {
                existing
                    .segments
                    .last()
                    .map(|segment| segment.ident == *ident)
                    .unwrap_or(false)
            });
            if !already_present {
                traits.push(path.clone());
            }
        }

        let updated: Attribute = parse_quote! {
            #[derive(#(#traits),*)]
        };
        item.attrs.insert(index, updated);
    } else {
        let attr: Attribute = parse_quote! {
            #[derive(Debug, Clone, ::serde::Serialize, ::serde::Deserialize, ::schemars::JsonSchema)]
        };
        item.attrs.insert(0, attr);
    }

    Ok(())
}

fn ensure_flow_enum_tag(item: &mut ItemEnum) {
    let mut has_tag = false;
    for attr in &item.attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }

        if let Ok(meta) = attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated) {
            for entry in meta {
                if let Meta::NameValue(name_value) = entry {
                    if name_value.path.is_ident("tag") {
                        if let Expr::Lit(ExprLit {
                            lit: Lit::Str(value),
                            ..
                        }) = name_value.value
                        {
                            if value.value() == "type" {
                                has_tag = true;
                            }
                        }
                    }
                }
            }
        }
    }

    if !has_tag {
        let attr: Attribute = parse_quote! {
            #[serde(tag = "type")]
        };
        item.attrs.push(attr);
    }
}

fn parse_effects(lit: &Lit) -> Result<ParsedEffects> {
    let value = lit_to_string(lit)?;
    let level = EffectLevel::parse(&value, lit.span())?;
    Ok(ParsedEffects::new(level, lit.span()))
}

fn parse_determinism(lit: &Lit) -> Result<ParsedDeterminism> {
    let value = lit_to_string(lit)?;
    let level = DeterminismLevel::parse(&value, lit.span())?;
    Ok(ParsedDeterminism::new(level, lit.span()))
}

fn parse_node_kind(lit: &Lit) -> Result<TokenStream2> {
    let value = lit_to_string(lit)?;
    match value.as_str() {
        "Trigger" | "trigger" => Ok(enum_expr("NodeKind", "Trigger", lit.span())),
        "Inline" | "inline" => Ok(enum_expr("NodeKind", "Inline", lit.span())),
        "Activity" | "activity" => Ok(enum_expr("NodeKind", "Activity", lit.span())),
        "Subflow" | "subflow" => Ok(enum_expr("NodeKind", "Subflow", lit.span())),
        other => Err(syn::Error::new(
            lit.span(),
            format!("[DAG003] unknown node kind `{other}`"),
        )),
    }
}

fn enum_expr(enum_name: &str, variant: &str, span: Span) -> TokenStream2 {
    let enum_ident = Ident::new(enum_name, span);
    let variant_ident = Ident::new(variant, span);
    quote!(::dag_core::#enum_ident::#variant_ident)
}

fn lit_to_string(lit: &Lit) -> Result<String> {
    if let Lit::Str(s) = lit {
        Ok(s.value())
    } else {
        Err(syn::Error::new(
            lit.span(),
            "value must be specified as string literal",
        ))
    }
}

struct WorkflowInput {
    name: Ident,
    version: LitStr,
    profile: Ident,
    summary: Option<LitStr>,
    bindings: Vec<Binding>,
    connects: Vec<ConnectEntry>,
}

struct Binding {
    alias: Ident,
    expr: Expr,
}

struct ConnectEntry {
    from: Ident,
    to: Ident,
}

struct ConnectArgs {
    from: Ident,
    to: Ident,
}

impl Parse for ConnectArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        let from: Ident = input.parse()?;
        input.parse::<Token![->]>()?;
        let to: Ident = input.parse()?;
        if !input.is_empty() {
            return Err(syn::Error::new(
                input.span(),
                "connect! currently supports only `from -> to` syntax",
            ));
        }
        Ok(Self { from, to })
    }
}

impl Parse for WorkflowInput {
    fn parse(input: ParseStream) -> Result<Self> {
        let mut name = None;
        let mut version = None;
        let mut profile = None;
        let mut summary = None;
        let mut bindings = Vec::new();
        let mut connects = Vec::new();

        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<Token![:]>()?;
            match key.to_string().as_str() {
                "name" => {
                    if name.is_some() {
                        return Err(syn::Error::new(key.span(), "duplicate `name` field"));
                    }
                    name = Some(input.parse()?);
                }
                "version" => {
                    if version.is_some() {
                        return Err(syn::Error::new(key.span(), "duplicate `version` field"));
                    }
                    version = Some(input.parse()?);
                }
                "profile" => {
                    if profile.is_some() {
                        return Err(syn::Error::new(key.span(), "duplicate `profile` field"));
                    }
                    profile = Some(input.parse()?);
                }
                "summary" => {
                    summary = Some(input.parse()?);
                }
                other => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!("unknown workflow field `{other}`"),
                    ));
                }
            }

            if input.peek(Token![;]) {
                input.parse::<Token![;]>()?;
                break;
            } else if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            } else {
                return Err(syn::Error::new(
                    input.span(),
                    "expected `,` or `;` after workflow metadata",
                ));
            }
        }

        while !input.is_empty() {
            if input.peek(Token![let]) {
                input.parse::<Token![let]>()?;
                let alias: Ident = input.parse()?;
                input.parse::<Token![=]>()?;
                let expr: Expr = input.parse()?;
                input.parse::<Token![;]>()?;
                bindings.push(Binding { alias, expr });
                continue;
            }

            if input.peek(Token![if])
                || input.peek(Token![match])
                || input.peek(Token![while])
                || input.peek(Token![for])
            {
                return Err(syn::Error::new(
                    input.span(),
                    "workflow! does not support Rust control-flow statements; use the flow::switch/for_each helpers",
                ));
            }

            let mac: Macro = input.parse()?;
            if !mac.path.is_ident("connect") {
                return Err(syn::Error::new(
                    mac.span(),
                    "workflow! body currently supports only `let` bindings and `connect!` statements",
                ));
            }
            let args = syn::parse2::<ConnectArgs>(mac.tokens)?;
            if input.peek(Token![;]) {
                input.parse::<Token![;]>()?;
            } else {
                return Err(syn::Error::new(
                    input.span(),
                    "expected `;` after connect! statement",
                ));
            }
            connects.push(ConnectEntry {
                from: args.from,
                to: args.to,
            });
        }

        Ok(WorkflowInput {
            name: name
                .ok_or_else(|| syn::Error::new(Span::call_site(), "workflow! requires `name`"))?,
            version: version.ok_or_else(|| {
                syn::Error::new(Span::call_site(), "workflow! requires `version`")
            })?,
            profile: profile.ok_or_else(|| {
                syn::Error::new(Span::call_site(), "workflow! requires `profile`")
            })?,
            summary,
            bindings,
            connects,
        })
    }
}

impl WorkflowInput {
    fn expand(&self) -> Result<TokenStream2> {
        let fn_name = &self.name;
        let flow_name = self.name.to_string();
        let flow_name_lit = LitStr::new(&flow_name, self.name.span());
        let profile_ident = &self.profile;
        let version_literal = &self.version;

        Version::parse(&version_literal.value()).map_err(|err| {
            syn::Error::new(
                version_literal.span(),
                format!("invalid semver literal: {err}"),
            )
        })?;

        let mut alias_set = HashSet::new();
        for binding in &self.bindings {
            let alias_str = binding.alias.to_string();
            if !alias_set.insert(alias_str.clone()) {
                return Err(syn::Error::new(
                    binding.alias.span(),
                    format!("[DAG205] duplicate node alias `{}`", binding.alias),
                ));
            }
        }

        for connect in &self.connects {
            let from = connect.from.to_string();
            let to = connect.to.to_string();
            if !alias_set.contains(&from) {
                return Err(syn::Error::new(
                    connect.from.span(),
                    format!("[DAG202] unknown node alias `{}`", connect.from),
                ));
            }
            if !alias_set.contains(&to) {
                return Err(syn::Error::new(
                    connect.to.span(),
                    format!("[DAG202] unknown node alias `{}`", connect.to),
                ));
            }
        }

        let summary_stmt = if let Some(summary) = &self.summary {
            quote!(builder.summary(Some(#summary));)
        } else {
            quote!()
        };

        let binding_statements = self.bindings.iter().map(|binding| {
            let alias = &binding.alias;
            let alias_str = binding.alias.to_string();
            let expr = &binding.expr;
            quote! {
                let #alias = builder
                    .add_node(#alias_str, #expr)
                    .expect(concat!(
                        "[DAG205] duplicate node alias `",
                        stringify!(#alias),
                        "`"
                    ));
            }
        });

        let connect_statements = self.connects.iter().map(|connect| {
            let from = &connect.from;
            let to = &connect.to;
            quote! {
                builder.connect(&#from, &#to);
            }
        });

        Ok(quote! {
            pub fn #fn_name() -> ::dag_core::FlowIR {
                let version = ::dag_core::prelude::Version::parse(#version_literal)
                    .expect("workflow!: invalid semver literal");
                let mut builder = ::dag_core::FlowBuilder::new(
                    #flow_name_lit,
                    version,
                    ::dag_core::Profile::#profile_ident,
                );

                #summary_stmt
                #(#binding_statements)*
                #(#connect_statements)*

                builder.build()
            }
        })
    }
}
