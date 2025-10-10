// TODO
use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{ToTokens, format_ident, quote};
use semver::Version;
use std::collections::HashSet;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{
    Expr, ExprLit, Ident, ItemFn, Lit, LitStr, Meta, MetaNameValue, Result, Token, braced,
    bracketed, parse_macro_input, spanned::Spanned,
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

struct NodeDefaults {
    kind: &'static str,
    effects: &'static str,
    determinism: &'static str,
}

impl NodeDefaults {
    fn node() -> Self {
        Self {
            kind: "Inline",
            effects: "Pure",
            determinism: "BestEffort",
        }
    }

    fn trigger() -> Self {
        Self {
            kind: "Trigger",
            effects: "ReadOnly",
            determinism: "Strict",
        }
    }
}

struct NodeArgs {
    name: Option<LitStr>,
    summary: Option<LitStr>,
    effects: Option<TokenStream2>,
    determinism: Option<TokenStream2>,
    kind: Option<TokenStream2>,
    input_schema: Option<LitStr>,
    output_schema: Option<LitStr>,
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
                _ => {
                    return Err(syn::Error::new(
                        meta.span(),
                        "expected key = \"value\" pairs in attribute",
                    ));
                }
            }
        }

        if parsed.effects.is_none() {
            parsed.effects = Some(enum_expr("Effects", defaults.effects, Span::call_site()));
        }
        if parsed.determinism.is_none() {
            parsed.determinism = Some(enum_expr(
                "Determinism",
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

    let effects_expr = config.effects.unwrap();
    let determinism_expr = config.determinism.unwrap();
    let kind_expr = config.kind.unwrap();

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
        };

        #[allow(dead_code)]
        pub fn #accessor_ident() -> &'static ::dag_core::NodeSpec {
            &#spec_ident
        }
    })
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

fn parse_effects(lit: &Lit) -> Result<TokenStream2> {
    let value = lit_to_string(lit)?;
    match value.as_str() {
        "Pure" | "pure" => Ok(enum_expr("Effects", "Pure", lit.span())),
        "ReadOnly" | "readonly" => Ok(enum_expr("Effects", "ReadOnly", lit.span())),
        "Effectful" | "effectful" => Ok(enum_expr("Effects", "Effectful", lit.span())),
        other => Err(syn::Error::new(
            lit.span(),
            format!("[EFFECT201] unknown effects value `{other}`"),
        )),
    }
}

fn parse_determinism(lit: &Lit) -> Result<TokenStream2> {
    let value = lit_to_string(lit)?;
    match value.as_str() {
        "Strict" | "strict" => Ok(enum_expr("Determinism", "Strict", lit.span())),
        "Stable" | "stable" => Ok(enum_expr("Determinism", "Stable", lit.span())),
        "BestEffort" | "besteffort" | "best_effort" => {
            Ok(enum_expr("Determinism", "BestEffort", lit.span()))
        }
        "Nondeterministic" | "non_deterministic" | "nondet" => {
            Ok(enum_expr("Determinism", "Nondeterministic", lit.span()))
        }
        other => Err(syn::Error::new(
            lit.span(),
            format!("[DET301] unknown determinism value `{other}`"),
        )),
    }
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
    nodes: Vec<NodeEntry>,
    edges: Vec<EdgeEntry>,
}

struct NodeEntry {
    alias: Ident,
    expr: Expr,
}

struct EdgeEntry {
    from: Ident,
    to: Ident,
}

impl Parse for WorkflowInput {
    fn parse(input: ParseStream) -> Result<Self> {
        let mut name = None;
        let mut version = None;
        let mut profile = None;
        let mut summary = None;
        let mut nodes = None;
        let mut edges = None;

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
                "nodes" => {
                    if nodes.is_some() {
                        return Err(syn::Error::new(key.span(), "duplicate `nodes` block"));
                    }
                    let nodes_content;
                    braced!(nodes_content in input);
                    let mut entries = Vec::new();
                    while !nodes_content.is_empty() {
                        let alias: Ident = nodes_content.parse()?;
                        nodes_content.parse::<Token![=>]>()?;
                        let expr: Expr = nodes_content.parse()?;
                        entries.push(NodeEntry { alias, expr });
                        if nodes_content.peek(Token![,]) {
                            nodes_content.parse::<Token![,]>()?;
                        }
                    }
                    nodes = Some(entries);
                }
                "edges" => {
                    if edges.is_some() {
                        return Err(syn::Error::new(key.span(), "duplicate `edges` block"));
                    }
                    let edges_content;
                    bracketed!(edges_content in input);
                    let mut entries = Vec::new();
                    while !edges_content.is_empty() {
                        let from: Ident = edges_content.parse()?;
                        edges_content.parse::<Token![=>]>()?;
                        let to: Ident = edges_content.parse()?;
                        entries.push(EdgeEntry { from, to });
                        if edges_content.peek(Token![,]) {
                            edges_content.parse::<Token![,]>()?;
                        }
                    }
                    edges = Some(entries);
                }
                other => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!("unknown workflow field `{other}`"),
                    ));
                }
            }

            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
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
            nodes: nodes.ok_or_else(|| {
                syn::Error::new(Span::call_site(), "workflow! requires `nodes` block")
            })?,
            edges: edges.unwrap_or_default(),
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

        let mut node_aliases = HashSet::new();
        for node in &self.nodes {
            if !node_aliases.insert(node.alias.to_string()) {
                return Err(syn::Error::new(
                    node.alias.span(),
                    format!("[DAG205] duplicate node alias `{}`", node.alias),
                ));
            }
        }

        let mut missing_nodes = Vec::new();
        for edge in &self.edges {
            if !node_aliases.contains(&edge.from.to_string()) {
                missing_nodes.push(edge.from.span());
            }
            if !node_aliases.contains(&edge.to.to_string()) {
                missing_nodes.push(edge.to.span());
            }
        }
        if let Some(span) = missing_nodes.first() {
            return Err(syn::Error::new(
                *span,
                "[DAG201] edge references undefined node",
            ));
        }

        let summary_stmt = if let Some(summary) = &self.summary {
            quote!(builder.summary(Some(#summary));)
        } else {
            quote!()
        };

        let node_statements = self.nodes.iter().map(|node| {
            let alias = &node.alias;
            let alias_lit = LitStr::new(&node.alias.to_string(), node.alias.span());
            let expr = &node.expr;
            quote! {
                let #alias = builder
                    .add_node(#alias_lit, #expr)
                    .expect(concat!("[DAG205] duplicate node alias `", stringify!(#alias), "`"));
            }
        });

        let edge_statements = self.edges.iter().map(|edge| {
            let from = &edge.from;
            let to = &edge.to;
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
                #(#node_statements)*
                #(#edge_statements)*

                builder.build()
            }
        })
    }
}
