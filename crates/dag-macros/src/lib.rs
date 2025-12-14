// TODO
use capabilities::hints;
use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{ToTokens, format_ident, quote};
use semver::Version;
use std::collections::HashSet;
use syn::braced;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{
    Attribute, Expr, ExprLit, Ident, ItemEnum, ItemFn, Lit, LitInt, LitStr, Macro, Meta,
    MetaNameValue, Path, Result, Token, parse_macro_input, parse_quote, spanned::Spanned,
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

        let inferred = hints::infer(&alias_lower, &cap_lower);

        for hint in inferred.determinism_hints {
            push_hint(
                &mut determinism,
                &mut determinism_seen,
                hint,
                &alias,
                resource.span,
            );
        }

        for hint in inferred.effect_hints {
            push_hint(&mut effects, &mut effects_seen, hint, &alias, resource.span);
        }

        if inferred.is_empty() {
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
            && !effects.level.to_runtime().is_at_least(constraint.minimum)
        {
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
    Ok(())
}

fn validate_determinism_hints(hints: &[HintSpec], determinism: &ParsedDeterminism) -> Result<()> {
    for hint in hints {
        if let Some(constraint) = dag_core::determinism::constraint_for_hint(hint.value.as_str())
            && !determinism
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
    if let syn::Type::Path(path) = ty
        && let Some(last) = path.path.segments.last()
        && last.ident == "NodeResult"
        && let syn::PathArguments::AngleBracketed(args) = &last.arguments
        && let Some(syn::GenericArgument::Type(inner)) = args.args.first()
    {
        return Ok(schema_from_type(inner));
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
                if let Meta::NameValue(name_value) = entry
                    && name_value.path.is_ident("tag")
                    && let Expr::Lit(ExprLit {
                        lit: Lit::Str(value),
                        ..
                    }) = name_value.value
                    && value.value() == "type"
                {
                    has_tag = true;
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
    timeouts: Vec<TimeoutEntry>,
    deliveries: Vec<DeliveryEntry>,
    buffers: Vec<BufferEntry>,
    spills: Vec<SpillEntry>,
    switches: Vec<SwitchEntry>,
}

struct Binding {
    alias: Ident,
    expr: Expr,
}

struct ConnectEntry {
    from: Ident,
    to: Ident,
}

struct TimeoutEntry {
    from: Ident,
    to: Ident,
    ms: u64,
    span: Span,
}

#[derive(Copy, Clone, Debug)]
enum DeliveryMode {
    AtLeast,
    AtMost,
    Exactly,
}

impl DeliveryMode {
    fn parse(value: &str, span: Span) -> Result<Self> {
        match value {
            "at_least_once" => Ok(DeliveryMode::AtLeast),
            "at_most_once" => Ok(DeliveryMode::AtMost),
            "exactly_once" => Ok(DeliveryMode::Exactly),
            other => Err(syn::Error::new(
                span,
                format!(
                    "unknown delivery mode `{other}` (expected `at_least_once|at_most_once|exactly_once`)"
                ),
            )),
        }
    }

    fn to_tokens(self, span: Span) -> TokenStream2 {
        match self {
            DeliveryMode::AtLeast => enum_expr("Delivery", "AtLeastOnce", span),
            DeliveryMode::AtMost => enum_expr("Delivery", "AtMostOnce", span),
            DeliveryMode::Exactly => enum_expr("Delivery", "ExactlyOnce", span),
        }
    }
}

struct DeliveryEntry {
    from: Ident,
    to: Ident,
    mode: DeliveryMode,
    span: Span,
}

struct BufferEntry {
    from: Ident,
    to: Ident,
    max_items: u32,
    span: Span,
}

struct SpillEntry {
    from: Ident,
    to: Ident,
    tier: LitStr,
    threshold_bytes: Option<u64>,
    span: Span,
}

struct SwitchEntry {
    source: Ident,
    selector_pointer: LitStr,
    cases: Vec<(LitStr, Ident)>,
    default: Option<Ident>,
    span: Span,
}

struct ConnectArgs {
    from: Ident,
    to: Ident,
}

struct TimeoutArgs {
    from: Ident,
    to: Ident,
    ms: u64,
}

struct DeliveryArgs {
    from: Ident,
    to: Ident,
    mode: DeliveryMode,
}

struct BufferArgs {
    from: Ident,
    to: Ident,
    max_items: u32,
}

struct SpillArgs {
    from: Ident,
    to: Ident,
    tier: LitStr,
    threshold_bytes: Option<u64>,
}

struct SwitchArgs {
    source: Ident,
    selector_pointer: LitStr,
    cases: Vec<(LitStr, Ident)>,
    default: Option<Ident>,
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

impl Parse for TimeoutArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        let from: Ident = input.parse()?;
        input.parse::<Token![->]>()?;
        let to: Ident = input.parse()?;

        input.parse::<Token![,]>()?;
        let key: Ident = input.parse()?;
        if key != "ms" {
            return Err(syn::Error::new(
                key.span(),
                "timeout! requires `ms = <integer>`",
            ));
        }
        input.parse::<Token![=]>()?;
        let ms_lit: LitInt = input.parse()?;
        let ms = ms_lit.base10_parse::<u64>().map_err(|err| {
            syn::Error::new(ms_lit.span(), format!("invalid timeout ms literal: {err}"))
        })?;

        if !input.is_empty() {
            return Err(syn::Error::new(
                input.span(),
                "timeout! supports only `from -> to, ms = <integer>` syntax",
            ));
        }

        Ok(Self { from, to, ms })
    }
}

impl Parse for DeliveryArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        let from: Ident = input.parse()?;
        input.parse::<Token![->]>()?;
        let to: Ident = input.parse()?;

        input.parse::<Token![,]>()?;
        let key: Ident = input.parse()?;
        if key != "mode" {
            return Err(syn::Error::new(
                key.span(),
                "delivery! requires `mode = at_least_once|at_most_once|exactly_once`",
            ));
        }
        input.parse::<Token![=]>()?;
        let mode_expr: Expr = input.parse()?;
        let mode_ident = match &mode_expr {
            Expr::Path(path) if path.path.is_ident("at_least_once") => {
                Ident::new("at_least_once", path.span())
            }
            Expr::Path(path) if path.path.is_ident("at_most_once") => {
                Ident::new("at_most_once", path.span())
            }
            Expr::Path(path) if path.path.is_ident("exactly_once") => {
                Ident::new("exactly_once", path.span())
            }
            Expr::Path(path) if path.path.segments.len() == 1 => {
                path.path.segments[0].ident.clone()
            }
            _ => {
                return Err(syn::Error::new_spanned(
                    mode_expr,
                    "delivery! requires `mode = at_least_once|at_most_once|exactly_once`",
                ));
            }
        };
        let mode = DeliveryMode::parse(&mode_ident.to_string(), mode_ident.span())?;

        if !input.is_empty() {
            return Err(syn::Error::new(
                input.span(),
                "delivery! supports only `from -> to, mode = <...>` syntax",
            ));
        }

        Ok(Self { from, to, mode })
    }
}

impl Parse for BufferArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        let from: Ident = input.parse()?;
        input.parse::<Token![->]>()?;
        let to: Ident = input.parse()?;

        input.parse::<Token![,]>()?;
        let key: Ident = input.parse()?;
        if key != "max_items" {
            return Err(syn::Error::new(
                key.span(),
                "buffer! requires `max_items = <integer>`",
            ));
        }
        input.parse::<Token![=]>()?;
        let max_items_expr: Expr = input.parse()?;
        let max_items_lit = match &max_items_expr {
            Expr::Lit(ExprLit {
                lit: Lit::Int(lit), ..
            }) => lit,
            _ => {
                return Err(syn::Error::new_spanned(
                    max_items_expr,
                    "buffer! requires `max_items = <integer>`",
                ));
            }
        };
        let max_items = max_items_lit.base10_parse::<u32>().map_err(|err| {
            syn::Error::new(
                max_items_lit.span(),
                format!("invalid max_items literal: {err}"),
            )
        })?;

        if !input.is_empty() {
            return Err(syn::Error::new(
                input.span(),
                "buffer! supports only `from -> to, max_items = <integer>` syntax",
            ));
        }

        Ok(Self {
            from,
            to,
            max_items,
        })
    }
}

impl Parse for SpillArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        let from: Ident = input.parse()?;
        input.parse::<Token![->]>()?;
        let to: Ident = input.parse()?;

        input.parse::<Token![,]>()?;
        let key: Ident = input.parse()?;
        if key != "tier" {
            return Err(syn::Error::new(
                key.span(),
                "spill! requires `tier = \"...\"`",
            ));
        }
        input.parse::<Token![=]>()?;
        let tier_expr: Expr = input.parse()?;
        let tier = match &tier_expr {
            Expr::Lit(ExprLit {
                lit: Lit::Str(lit), ..
            }) => lit.clone(),
            _ => {
                return Err(syn::Error::new_spanned(
                    tier_expr,
                    "spill! requires `tier = \"...\"`",
                ));
            }
        };

        let mut threshold_bytes = None;
        if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            let key: Ident = input.parse()?;
            if key != "threshold_bytes" {
                return Err(syn::Error::new(
                    key.span(),
                    "spill! supports only `tier = \"...\"[, threshold_bytes = <integer>]`",
                ));
            }
            input.parse::<Token![=]>()?;
            let threshold_expr: Expr = input.parse()?;
            let threshold_lit = match &threshold_expr {
                Expr::Lit(ExprLit {
                    lit: Lit::Int(lit), ..
                }) => lit,
                _ => {
                    return Err(syn::Error::new_spanned(
                        threshold_expr,
                        "spill! supports only `tier = \"...\"[, threshold_bytes = <integer>]`",
                    ));
                }
            };
            let threshold = threshold_lit.base10_parse::<u64>().map_err(|err| {
                syn::Error::new(
                    threshold_lit.span(),
                    format!("invalid threshold_bytes literal: {err}"),
                )
            })?;
            threshold_bytes = Some(threshold);
        }

        if !input.is_empty() {
            return Err(syn::Error::new(
                input.span(),
                "spill! supports only `from -> to, tier = \"...\"[, threshold_bytes = <integer>]` syntax",
            ));
        }

        Ok(Self {
            from,
            to,
            tier,
            threshold_bytes,
        })
    }
}

impl Parse for SwitchArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        let key: Ident = input.parse()?;
        if key != "source" {
            return Err(syn::Error::new(
                key.span(),
                "switch! requires `source = <node_alias>`",
            ));
        }
        input.parse::<Token![=]>()?;
        let source_expr: Expr = input.parse()?;
        let source = match &source_expr {
            Expr::Path(path) if path.path.segments.len() == 1 => {
                path.path.segments[0].ident.clone()
            }
            _ => {
                return Err(syn::Error::new_spanned(
                    source_expr,
                    "switch! requires `source = <node_alias>`",
                ));
            }
        };

        input.parse::<Token![,]>()?;
        let key: Ident = input.parse()?;
        if key != "selector_pointer" {
            return Err(syn::Error::new(
                key.span(),
                "switch! requires `selector_pointer = \"/json/pointer\"`",
            ));
        }
        input.parse::<Token![=]>()?;
        let selector_expr: Expr = input.parse()?;
        let selector_pointer = match &selector_expr {
            Expr::Lit(ExprLit {
                lit: Lit::Str(lit), ..
            }) => lit.clone(),
            _ => {
                return Err(syn::Error::new_spanned(
                    selector_expr,
                    "switch! requires `selector_pointer = \"/json/pointer\"`",
                ));
            }
        };

        input.parse::<Token![,]>()?;
        let key: Ident = input.parse()?;
        if key != "cases" {
            return Err(syn::Error::new(
                key.span(),
                "switch! requires `cases = { ... }`",
            ));
        }
        input.parse::<Token![=]>()?;

        let content;
        braced!(content in input);
        let mut cases = Vec::new();
        let mut seen = HashSet::new();
        while !content.is_empty() {
            let key: LitStr = content.parse()?;
            content.parse::<Token![=>]>()?;
            let value_expr: Expr = content.parse()?;
            let value = match &value_expr {
                Expr::Path(path) if path.path.segments.len() == 1 => {
                    path.path.segments[0].ident.clone()
                }
                _ => {
                    return Err(syn::Error::new_spanned(
                        value_expr,
                        "switch! cases must map to node aliases (e.g. `\"k\" => target`)",
                    ));
                }
            };

            if !seen.insert(key.value()) {
                return Err(syn::Error::new(
                    key.span(),
                    format!("duplicate switch case key `{}`", key.value()),
                ));
            }
            cases.push((key, value));

            if content.peek(Token![,]) {
                content.parse::<Token![,]>()?;
                if content.is_empty() {
                    break;
                }
            } else {
                break;
            }
        }

        let mut default = None;
        if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            let key: Ident = input.parse()?;
            if key != "default" {
                return Err(syn::Error::new(
                    key.span(),
                    "switch! supports only `default = <node_alias>` as an optional final argument",
                ));
            }
            input.parse::<Token![=]>()?;
            let default_expr: Expr = input.parse()?;
            let default_ident = match &default_expr {
                Expr::Path(path) if path.path.segments.len() == 1 => {
                    path.path.segments[0].ident.clone()
                }
                _ => {
                    return Err(syn::Error::new_spanned(
                        default_expr,
                        "switch! requires `default = <node_alias>`",
                    ));
                }
            };
            default = Some(default_ident);
        }

        if !input.is_empty() {
            return Err(syn::Error::new(
                input.span(),
                "switch! supports only `source = ..., selector_pointer = \"...\", cases = { ... }[, default = ...]` syntax",
            ));
        }

        Ok(Self {
            source,
            selector_pointer,
            cases,
            default,
        })
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
        let mut timeouts = Vec::new();
        let mut deliveries = Vec::new();
        let mut buffers = Vec::new();
        let mut spills = Vec::new();
        let mut switches = Vec::new();

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

            if mac.path.is_ident("connect") {
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
                continue;
            }

            if mac.path.is_ident("timeout") {
                let span = mac.span();
                let args = syn::parse2::<TimeoutArgs>(mac.tokens)?;
                if input.peek(Token![;]) {
                    input.parse::<Token![;]>()?;
                } else {
                    return Err(syn::Error::new(
                        input.span(),
                        "expected `;` after timeout! statement",
                    ));
                }
                timeouts.push(TimeoutEntry {
                    from: args.from,
                    to: args.to,
                    ms: args.ms,
                    span,
                });
                continue;
            }

            if mac.path.is_ident("delivery") {
                let span = mac.span();
                let args = syn::parse2::<DeliveryArgs>(mac.tokens)?;
                if input.peek(Token![;]) {
                    input.parse::<Token![;]>()?;
                } else {
                    return Err(syn::Error::new(
                        input.span(),
                        "expected `;` after delivery! statement",
                    ));
                }
                deliveries.push(DeliveryEntry {
                    from: args.from,
                    to: args.to,
                    mode: args.mode,
                    span,
                });
                continue;
            }

            if mac.path.is_ident("buffer") {
                let span = mac.span();
                let args = syn::parse2::<BufferArgs>(mac.tokens)?;
                if input.peek(Token![;]) {
                    input.parse::<Token![;]>()?;
                } else {
                    return Err(syn::Error::new(
                        input.span(),
                        "expected `;` after buffer! statement",
                    ));
                }
                buffers.push(BufferEntry {
                    from: args.from,
                    to: args.to,
                    max_items: args.max_items,
                    span,
                });
                continue;
            }

            if mac.path.is_ident("spill") {
                let span = mac.span();
                let args = syn::parse2::<SpillArgs>(mac.tokens)?;
                if input.peek(Token![;]) {
                    input.parse::<Token![;]>()?;
                } else {
                    return Err(syn::Error::new(
                        input.span(),
                        "expected `;` after spill! statement",
                    ));
                }
                spills.push(SpillEntry {
                    from: args.from,
                    to: args.to,
                    tier: args.tier,
                    threshold_bytes: args.threshold_bytes,
                    span,
                });
                continue;
            }

            if mac.path.is_ident("switch") {
                let span = mac.span();
                let args = syn::parse2::<SwitchArgs>(mac.tokens)?;
                if input.peek(Token![;]) {
                    input.parse::<Token![;]>()?;
                } else {
                    return Err(syn::Error::new(
                        input.span(),
                        "expected `;` after switch! statement",
                    ));
                }
                switches.push(SwitchEntry {
                    source: args.source,
                    selector_pointer: args.selector_pointer,
                    cases: args.cases,
                    default: args.default,
                    span,
                });
                continue;
            }

            return Err(syn::Error::new(
                mac.span(),
                "workflow! body currently supports only `let` bindings, `connect!`, `timeout!`, `delivery!`, `buffer!`, `spill!`, and `switch!` statements",
            ));
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
            timeouts,
            deliveries,
            buffers,
            spills,
            switches,
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

        let mut connected_edges = HashSet::new();
        for connect in &self.connects {
            connected_edges.insert((connect.from.to_string(), connect.to.to_string()));
        }

        let mut timeout_edges = HashSet::new();
        for timeout in &self.timeouts {
            let from = timeout.from.to_string();
            let to = timeout.to.to_string();

            if !alias_set.contains(&from) {
                return Err(syn::Error::new(
                    timeout.from.span(),
                    format!("[DAG202] unknown node alias `{}`", timeout.from),
                ));
            }
            if !alias_set.contains(&to) {
                return Err(syn::Error::new(
                    timeout.to.span(),
                    format!("[DAG202] unknown node alias `{}`", timeout.to),
                ));
            }

            if !connected_edges.contains(&(from.clone(), to.clone())) {
                return Err(syn::Error::new(
                    timeout.span,
                    format!("[DAG206] timeout! references missing edge `{from}` -> `{to}`"),
                ));
            }

            if !timeout_edges.insert((from.clone(), to.clone())) {
                return Err(syn::Error::new(
                    timeout.span,
                    format!("[DAG207] duplicate timeout! for edge `{from}` -> `{to}`"),
                ));
            }
        }

        let mut delivery_edges = HashSet::new();
        for delivery in &self.deliveries {
            let from = delivery.from.to_string();
            let to = delivery.to.to_string();

            if !alias_set.contains(&from) {
                return Err(syn::Error::new(
                    delivery.from.span(),
                    format!("[DAG202] unknown node alias `{}`", delivery.from),
                ));
            }
            if !alias_set.contains(&to) {
                return Err(syn::Error::new(
                    delivery.to.span(),
                    format!("[DAG202] unknown node alias `{}`", delivery.to),
                ));
            }

            if !connected_edges.contains(&(from.clone(), to.clone())) {
                return Err(syn::Error::new(
                    delivery.span,
                    format!("[DAG206] delivery! references missing edge `{from}` -> `{to}`"),
                ));
            }

            if !delivery_edges.insert((from.clone(), to.clone())) {
                return Err(syn::Error::new(
                    delivery.span,
                    format!("[DAG207] duplicate delivery! for edge `{from}` -> `{to}`"),
                ));
            }
        }

        let mut buffer_edges = HashSet::new();
        for buffer in &self.buffers {
            let from = buffer.from.to_string();
            let to = buffer.to.to_string();

            if !alias_set.contains(&from) {
                return Err(syn::Error::new(
                    buffer.from.span(),
                    format!("[DAG202] unknown node alias `{}`", buffer.from),
                ));
            }
            if !alias_set.contains(&to) {
                return Err(syn::Error::new(
                    buffer.to.span(),
                    format!("[DAG202] unknown node alias `{}`", buffer.to),
                ));
            }

            if !connected_edges.contains(&(from.clone(), to.clone())) {
                return Err(syn::Error::new(
                    buffer.span,
                    format!("[DAG206] buffer! references missing edge `{from}` -> `{to}`"),
                ));
            }

            if !buffer_edges.insert((from.clone(), to.clone())) {
                return Err(syn::Error::new(
                    buffer.span,
                    format!("[DAG207] duplicate buffer! for edge `{from}` -> `{to}`"),
                ));
            }
        }

        let mut spill_edges = HashSet::new();
        for spill in &self.spills {
            let from = spill.from.to_string();
            let to = spill.to.to_string();

            if !alias_set.contains(&from) {
                return Err(syn::Error::new(
                    spill.from.span(),
                    format!("[DAG202] unknown node alias `{}`", spill.from),
                ));
            }
            if !alias_set.contains(&to) {
                return Err(syn::Error::new(
                    spill.to.span(),
                    format!("[DAG202] unknown node alias `{}`", spill.to),
                ));
            }

            if !connected_edges.contains(&(from.clone(), to.clone())) {
                return Err(syn::Error::new(
                    spill.span,
                    format!("[DAG206] spill! references missing edge `{from}` -> `{to}`"),
                ));
            }

            if !spill_edges.insert((from.clone(), to.clone())) {
                return Err(syn::Error::new(
                    spill.span,
                    format!("[DAG207] duplicate spill! for edge `{from}` -> `{to}`"),
                ));
            }
        }

        let mut switch_sources = HashSet::new();
        for surface in &self.switches {
            let source = surface.source.to_string();
            if !alias_set.contains(&source) {
                return Err(syn::Error::new(
                    surface.source.span(),
                    format!("[DAG202] unknown node alias `{}`", surface.source),
                ));
            }

            if !switch_sources.insert(source.clone()) {
                return Err(syn::Error::new(
                    surface.span,
                    format!("[DAG207] duplicate switch! for source `{source}`"),
                ));
            }

            if surface.cases.is_empty() {
                return Err(syn::Error::new(
                    surface.span,
                    "switch! requires at least one case",
                ));
            }

            let pointer = surface.selector_pointer.value();
            if !(pointer.is_empty() || pointer.starts_with('/')) {
                return Err(syn::Error::new(
                    surface.selector_pointer.span(),
                    "switch! selector_pointer must be a JSON Pointer (empty string or starting with `/`)",
                ));
            }

            for (_case_key, target) in &surface.cases {
                let target_str = target.to_string();
                if !alias_set.contains(&target_str) {
                    return Err(syn::Error::new(
                        target.span(),
                        format!("[DAG202] unknown node alias `{}`", target),
                    ));
                }
                if !connected_edges.contains(&(source.clone(), target_str.clone())) {
                    return Err(syn::Error::new(
                        surface.span,
                        format!(
                            "[DAG206] switch! references missing edge `{}` -> `{}`",
                            source, target_str
                        ),
                    ));
                }
            }

            if let Some(default) = &surface.default {
                let target_str = default.to_string();
                if !alias_set.contains(&target_str) {
                    return Err(syn::Error::new(
                        default.span(),
                        format!("[DAG202] unknown node alias `{}`", default),
                    ));
                }
                if !connected_edges.contains(&(source.clone(), target_str.clone())) {
                    return Err(syn::Error::new(
                        surface.span,
                        format!(
                            "[DAG206] switch! references missing edge `{}` -> `{}`",
                            source, target_str
                        ),
                    ));
                }
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

        let timeout_statements = self.timeouts.iter().map(|timeout| {
            let from = &timeout.from;
            let to = &timeout.to;
            let ms = timeout.ms;
            quote! {
                builder
                    .set_edge_timeout_ms(&#from, &#to, #ms)
                    .expect("timeout! references existing edge");
            }
        });

        let delivery_statements = self.deliveries.iter().map(|delivery| {
            let from = &delivery.from;
            let to = &delivery.to;
            let mode = delivery.mode.to_tokens(delivery.span);
            quote! {
                builder
                    .set_edge_delivery(&#from, &#to, #mode)
                    .expect("delivery! references existing edge");
            }
        });

        let buffer_statements = self.buffers.iter().map(|buffer| {
            let from = &buffer.from;
            let to = &buffer.to;
            let max_items = buffer.max_items;
            quote! {
                builder
                    .set_edge_buffer_max_items(&#from, &#to, #max_items)
                    .expect("buffer! references existing edge");
            }
        });

        let spill_statements = self.spills.iter().map(|spill| {
            let from = &spill.from;
            let to = &spill.to;
            let tier = &spill.tier;
            let threshold_stmt = spill.threshold_bytes.map(|threshold_bytes| {
                quote! {
                    builder
                        .set_edge_spill_threshold_bytes(&#from, &#to, #threshold_bytes)
                        .expect("spill! references existing edge");
                }
            });
            quote! {
                builder
                    .set_edge_spill_tier(&#from, &#to, #tier)
                    .expect("spill! references existing edge");
                #threshold_stmt
            }
        });

        let switch_statements = self.switches.iter().enumerate().map(|(idx, surface)| {
            let source_alias = surface.source.to_string();
            let source_alias_lit = LitStr::new(&source_alias, surface.source.span());
            let selector_pointer = &surface.selector_pointer;

            let mut target_aliases: Vec<String> = Vec::new();
            for (_key, target) in &surface.cases {
                let alias = target.to_string();
                if !target_aliases.contains(&alias) {
                    target_aliases.push(alias);
                }
            }
            if let Some(default) = &surface.default {
                let alias = default.to_string();
                if !target_aliases.contains(&alias) {
                    target_aliases.push(alias);
                }
            }

            let mut target_lits: Vec<LitStr> = Vec::new();
            for alias in &target_aliases {
                target_lits.push(LitStr::new(alias, surface.span));
            }

            let surface_id = format!("switch:{source_alias}:{idx}");
            let surface_id_lit = LitStr::new(&surface_id, surface.span);

            let case_inserts = surface.cases.iter().map(|(case_key, target)| {
                let target_alias = target.to_string();
                let target_alias_lit = LitStr::new(&target_alias, target.span());
                quote! {
                    cases.insert(#case_key.to_string(), ::dag_core::serde_json::Value::String(#target_alias_lit.to_string()));
                }
            });

            let default_insert = surface.default.as_ref().map(|default| {
                let default_alias = default.to_string();
                let default_alias_lit = LitStr::new(&default_alias, default.span());
                quote! {
                    config.insert(
                        "default".to_string(),
                        ::dag_core::serde_json::Value::String(#default_alias_lit.to_string()),
                    );
                }
            });

            quote! {
                {
                    let mut cases = ::dag_core::serde_json::Map::new();
                    #(#case_inserts)*

                    let mut targets = Vec::new();
                    targets.push(#source_alias_lit.to_string());
                    #(targets.push(#target_lits.to_string());)*

                    let mut config = ::dag_core::serde_json::Map::new();
                    config.insert(
                        "v".to_string(),
                        ::dag_core::serde_json::Value::Number(::dag_core::serde_json::Number::from(1)),
                    );
                    config.insert(
                        "source".to_string(),
                        ::dag_core::serde_json::Value::String(#source_alias_lit.to_string()),
                    );
                    config.insert(
                        "selector_pointer".to_string(),
                        ::dag_core::serde_json::Value::String(#selector_pointer.to_string()),
                    );
                    config.insert(
                        "cases".to_string(),
                        ::dag_core::serde_json::Value::Object(cases),
                    );
                    #default_insert

                    flow.control_surfaces.push(::dag_core::ControlSurfaceIR {
                        id: #surface_id_lit.to_string(),
                        kind: ::dag_core::ControlSurfaceKind::Switch,
                        targets,
                        config: ::dag_core::serde_json::Value::Object(config),
                    });
                }
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
                #(#delivery_statements)*
                #(#buffer_statements)*
                #(#spill_statements)*
                #(#timeout_statements)*

                let mut flow = builder.build();
                #(#switch_statements)*
                flow
            }
        })
    }
}
